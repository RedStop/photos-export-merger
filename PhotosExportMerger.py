from AbstractMediaMerger import (AbstractMediaMerger, WriteStrategy,
                                  MediaFileInfo, MergeStats, _resolve_gps,
                                  TimezoneOverride, JpegSkipTimerange)
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from io import BytesIO
from JsonFileIdentifier import JsonFileFinder
from pathlib import Path
from PIL import Image
from sortedcontainers import SortedSet
from typing import Dict, List, Optional, Any
import exiftool
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile


DIRECT_WRITE_EXTS = {'.jpg', '.jpeg', '.tiff', '.tif', '.dng', '.cr2', '.heic'}
PARTIAL_WRITE_EXTS = {'.png', '.gif'}
VIDEO_EXTS = {'.avi', '.mkv', '.mov', '.mp4', '.m4v', '.webm'}
# QuickTime-based containers where ExifTool can write QT/UserData/XMP tags
# directly into the file.  Non-QT video containers (AVI, MKV, WebM) are
# copy-only; all metadata lives in the XMP sidecar.
QUICKTIME_VIDEO_EXTS = {'.mov', '.mp4', '.m4v'}
ALL_MEDIA_EXTS = DIRECT_WRITE_EXTS | PARTIAL_WRITE_EXTS | VIDEO_EXTS

# JPEG file extensions eligible for quality-based recompression.
JPEG_EXTENSIONS = {'.jpg', '.jpeg', '.jpe', '.jfif'}

# Extension groups that are interchangeable — ExifTool handles them the same
# way, so a "mismatch" within a group is not a real problem.
_EQUIVALENT_EXTS = [
    {'.jpg', '.jpeg'},
    {'.tif', '.tiff'},
    {'.mov', '.mp4', '.m4v', '.qt'},
]
# Build a lookup: extension → frozenset of its equivalents.
_EXT_EQUIV_MAP: Dict[str, frozenset] = {}
for _group in _EQUIVALENT_EXTS:
    _frozen = frozenset(_group)
    for _ext in _group:
        _EXT_EQUIV_MAP[_ext] = _frozen


def _is_real_ext_mismatch(source_ext: str, actual_ext: str) -> bool:
    """Return True only if the extensions are genuinely incompatible.

    Returns False when both extensions belong to the same equivalence group
    (e.g. .mov/.mp4, .jpg/.jpeg) since ExifTool handles them identically.
    """
    equiv = _EXT_EQUIV_MAP.get(source_ext)
    if equiv and actual_ext in equiv:
        return False
    return True

DATE_TAGS_PRIORITY = [
    'EXIF:DateTimeOriginal',
    'EXIF:CreateDate',
    'QuickTime:CreateDate',
    'QuickTime:MediaCreateDate',
    'EXIF:ModifyDate',
]

DESC_READ_TAGS = ['EXIF:UserComment', 'EXIF:ImageDescription', 'XMP:Description', 'IPTC:Caption-Abstract']

# Date tags that should be updated to resolved_datetime only when they
# already exist in the source file.  Applies to DIRECT and
# PARTIAL_WITH_SIDECAR strategies only (not video).
#
# Keys   = tag names as returned by ExifTool get_tags() (used for reading).
# Values = callable(dt_str, tz_str) → list of ExifTool write parameters.
#
# ExifTool returns XMP tags under the "XMP:" family-1 group regardless of
# the specific XMP namespace, so reads use "XMP:…" while writes must use
# the fully-qualified "XMP-photoshop:…" / "XMP-xmp:…" form.
CONDITIONAL_DATE_TAGS: Dict[str, Any] = {
    'XMP:DateCreated':          lambda dt, tz: [f'-XMP-photoshop:DateCreated={dt}{tz}'],
    'XMP:CreateDate':           lambda dt, tz: [f'-XMP-xmp:CreateDate={dt}{tz}'],
    'XMP:MetadataDate':         lambda dt, tz: [f'-XMP-xmp:MetadataDate={dt}{tz}'],
    'XMP:ModifyDate':           lambda dt, tz: [f'-XMP-xmp:ModifyDate={dt}{tz}'],
    'XMP:DateTimeOriginal':     lambda dt, tz: [f'-XMP-exif:DateTimeOriginal={dt}{tz}'],
    'XMP:DateTimeDigitized':    lambda dt, tz: [f'-XMP-exif:DateTimeDigitized={dt}{tz}'],
    'IPTC:DateCreated':         lambda dt, tz: [f'-IPTC:DateCreated={dt[:10]}'],
    'IPTC:TimeCreated':         lambda dt, tz: [f'-IPTC:TimeCreated={dt[11:]}{tz}'],
    'IPTC:DigitalCreationDate': lambda dt, tz: [f'-IPTC:DigitalCreationDate={dt[:10]}'],
    'IPTC:DigitalCreationTime': lambda dt, tz: [f'-IPTC:DigitalCreationTime={dt[11:]}{tz}'],
}

# Read-tag list for batch reads (keys of the mapping above).
CONDITIONAL_DATE_READ_TAGS: List[str] = list(CONDITIONAL_DATE_TAGS.keys())


# ---------------------------------------------------------------------------
# Metadata stripping profiles
# ---------------------------------------------------------------------------
# Each profile maps a name to a list of ExifTool params that delete unwanted
# metadata groups.  Profiles are enabled via --strip-metadata on the CLI.
# To add a new profile, add an entry here — it will automatically appear in
# the CLI help and be included when --strip-metadata is used.
METADATA_STRIP_PROFILES: Dict[str, List[str]] = {
    'google':    ['-XMP-GCamera:All=', '-Google:All='],
    'photoshop': ['-Photoshop:All=', '-XMP-photoshop:DocumentAncestors='],
}

# ---------------------------------------------------------------------------
# Each editor maps a name to a dict with 'match' substrings (any one must
# appear) and optional 'exclude' substrings (none may appear).  Matching is
# performed case-insensitively against EXIF:Software and XMP-xmp:CreatorTool.
# Editors are selected via --jpeg-quality-skip-editor on the CLI.
# To add a new editor, add an entry here — it will automatically appear in
# the output of --list-editors.
EDITOR_SOFTWARE_PATTERNS: Dict[str, Dict[str, List[str]]] = {
    'lightroom':   {'match': ['Lightroom']},
    'photoshop':   {'match': ['Photoshop'], 'exclude': ['Lightroom']},
    'capture-one': {'match': ['Capture One']},
    'dxo':         {'match': ['DxO PhotoLab', 'DxO OpticsPro']},
    'on1':         {'match': ['ON1 Photo RAW']},
    'luminar':     {'match': ['Luminar']},
    'darktable':   {'match': ['darktable']},
    'rawtherapee': {'match': ['RawTherapee']},
    'gimp':        {'match': ['GIMP']},
    'affinity':    {'match': ['Affinity Photo']},
    'snapseed':    {'match': ['Snapseed']},
}


def _build_strip_params(profiles: Optional[List[str]] = None) -> Optional[List[str]]:
    """Build a combined ExifTool param list from the requested strip profiles.

    If *profiles* is None or empty, returns None (stripping disabled).
    The special name ``'all'`` enables every known profile.
    """
    if not profiles:
        return None

    params: List[str] = []
    if 'all' in profiles:
        for profile_params in METADATA_STRIP_PROFILES.values():
            params.extend(profile_params)
    else:
        for name in profiles:
            profile_params = METADATA_STRIP_PROFILES.get(name)
            if profile_params:
                params.extend(profile_params)

    return params if params else None


def _resolve_editor_skip_patterns(
    editor_names: List[str],
) -> List[Dict[str, List[str]]]:
    """Resolve user-supplied editor names to a list of pattern dicts.

    Each name is matched (case-insensitive substring) against
    ``EDITOR_SOFTWARE_PATTERNS`` keys.  The special name ``'all'`` selects
    every known editor.  Returns a deduplicated list of pattern dicts.
    Raises ``ValueError`` if a name matches no key.
    """
    if 'all' in (n.lower() for n in editor_names):
        return list(EDITOR_SOFTWARE_PATTERNS.values())

    seen_keys: set = set()
    result: List[Dict[str, List[str]]] = []
    for user_name in editor_names:
        hits = [(k, v) for k, v in EDITOR_SOFTWARE_PATTERNS.items()
                if user_name.lower() in k.lower()]
        if not hits:
            raise ValueError(
                f"No editor matching '{user_name}'. "
                f"Use --list-editors to see options.")
        for key, pattern in hits:
            if key not in seen_keys:
                seen_keys.add(key)
                result.append(pattern)
    return result


def _matches_editor_pattern(tag_value: str,
                            pattern: Dict[str, List[str]]) -> bool:
    """Return True if *tag_value* matches *pattern* (match + exclude logic).

    A match requires any ``match`` substring to be present **and** no
    ``exclude`` substring to be present (all case-insensitive).
    """
    val_lower = tag_value.lower()
    if not any(m.lower() in val_lower for m in pattern['match']):
        return False
    for excl in pattern.get('exclude', []):
        if excl.lower() in val_lower:
            return False
    return True


def _check_editor_skip(tags: dict, info: MediaFileInfo,
                       patterns: List[Dict[str, List[str]]]) -> None:
    """Set ``info.jpeg_skip_editor`` if Software/CreatorTool matches."""
    if info.source_path.suffix.lower() not in JPEG_EXTENSIONS:
        return
    software = str(tags.get('EXIF:Software', ''))
    creator = str(tags.get('XMP-xmp:CreatorTool', ''))
    for pattern in patterns:
        if ((software and _matches_editor_pattern(software, pattern)) or
                (creator and _matches_editor_pattern(creator, pattern))):
            info.jpeg_skip_editor = True
            return


def _check_jpeg_skip_timerange(info: MediaFileInfo,
                                timeranges: List[JpegSkipTimerange]) -> None:
    """Set ``info.jpeg_skip_timerange`` if resolved datetime is within a range."""
    if info.resolved_datetime is None:
        return
    utc_dt = info.resolved_datetime.astimezone(timezone.utc)
    for tr in timeranges:
        if tr.start_utc <= utc_dt <= tr.end_utc:
            info.jpeg_skip_timerange = True
            return


def _get_write_strategy(ext: str) -> Optional[WriteStrategy]:
    ext_lower = ext.lower()
    if ext_lower in DIRECT_WRITE_EXTS:
        return WriteStrategy.DIRECT
    elif ext_lower in PARTIAL_WRITE_EXTS:
        return WriteStrategy.PARTIAL_WITH_SIDECAR
    elif ext_lower in VIDEO_EXTS:
        return WriteStrategy.VIDEO_WITH_SIDECAR
    return None


def _parse_tz_offset(offset_str: str) -> Optional[timezone]:
    """Parse an EXIF timezone offset string like '+02:00' or '-05:00' into a timezone."""
    if not offset_str or not isinstance(offset_str, str):
        return None
    offset_str = offset_str.strip()
    try:
        sign = 1 if offset_str[0] == '+' else -1
        parts = offset_str[1:].split(':')
        hours = int(parts[0])
        minutes = int(parts[1]) if len(parts) > 1 else 0
        return timezone(timedelta(hours=sign * hours, minutes=sign * minutes))
    except (ValueError, IndexError):
        return None


def _format_tz_offset(tz: timezone) -> str:
    """Format a timezone to '+HH:MM' string."""
    offset = tz.utcoffset(None)
    total_seconds = int(offset.total_seconds())
    sign = '+' if total_seconds >= 0 else '-'
    total_seconds = abs(total_seconds)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    return f"{sign}{hours:02d}:{minutes:02d}"


def _find_tz_override(utc_dt: datetime,
                      overrides: List[TimezoneOverride]) -> Optional[timezone]:
    """Return the timezone from the first override whose range contains *utc_dt*.

    Returns None if no override matches.
    """
    for ov in overrides:
        if ov.start_utc <= utc_dt <= ov.end_utc:
            return ov.tz
    return None


def _parse_tz_override(value: str) -> TimezoneOverride:
    """Parse a --tz-override CLI value.

    Format: "YYYY-MM-DD HH:MM:SS,YYYY-MM-DD HH:MM:SS,+HH:MM"
    Times are UTC.  Returns a TimezoneOverride instance.
    """
    parts = value.split(',')
    if len(parts) != 3:
        raise ValueError(
            f"Expected 'START_UTC,END_UTC,OFFSET' but got: {value!r}")
    start_str, end_str, tz_str = [p.strip() for p in parts]
    try:
        start = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError(f"Invalid start UTC datetime: {start_str!r}")
    try:
        end = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError(f"Invalid end UTC datetime: {end_str!r}")
    tz = _parse_tz_offset(tz_str)
    if tz is None:
        raise ValueError(f"Invalid timezone offset: {tz_str!r}")
    if start > end:
        raise ValueError(
            f"Start UTC ({start_str}) is after end UTC ({end_str})")
    return TimezoneOverride(start_utc=start, end_utc=end, tz=tz)


def _parse_jpeg_skip_timerange(value: str) -> JpegSkipTimerange:
    """Parse a --jpeg-quality-skip-timerange CLI value.

    Format: "YYYY-MM-DD HH:MM:SS,YYYY-MM-DD HH:MM:SS,+HH:MM"
    The start/end datetimes are in the timezone given by the third field.
    They are converted to UTC for storage/comparison.
    """
    parts = value.split(',')
    if len(parts) != 3:
        raise ValueError(
            f"Expected 'START,END,OFFSET' but got: {value!r}")
    start_str, end_str, tz_str = [p.strip() for p in parts]
    tz = _parse_tz_offset(tz_str)
    if tz is None:
        raise ValueError(f"Invalid timezone offset: {tz_str!r}")
    try:
        start = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=tz)
    except ValueError:
        raise ValueError(f"Invalid start datetime: {start_str!r}")
    try:
        end = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=tz)
    except ValueError:
        raise ValueError(f"Invalid end datetime: {end_str!r}")
    start_utc = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)
    if start_utc > end_utc:
        raise ValueError(
            f"Start ({start_str}) is after end ({end_str}) in timezone {tz_str}")
    return JpegSkipTimerange(start_utc=start_utc, end_utc=end_utc)


def _build_batch_read_tags(base_tags: List[str],
                           blocked_descriptions,
                           jpeg_compress_quality: Optional[int],
                           editor_skip_patterns: List) -> List[str]:
    """Build the tag list for a batch ExifTool get_tags() call.

    Starts with *base_tags*, adds shared tags (CONDITIONAL_DATE_READ_TAGS,
    File:FileTypeExtension, description/IPTC tags), and conditionally adds
    JPEG quality and editor software tags.
    """
    tags = list(base_tags) + CONDITIONAL_DATE_READ_TAGS + ['File:FileTypeExtension']
    tags += DESC_READ_TAGS if blocked_descriptions else ['IPTC:Caption-Abstract']
    if jpeg_compress_quality is not None:
        tags.append('File:JPEGQualityEstimate')
    if editor_skip_patterns:
        tags.extend(['EXIF:Software', 'XMP-xmp:CreatorTool'])
    return tags


def _extract_common_tags(info: MediaFileInfo, tags: dict,
                         stats: MergeStats,
                         jpeg_compress_quality: Optional[int],
                         editor_skip_patterns: List,
                         blocked_descriptions,
                         logger: logging.Logger,
                         rel_fn) -> None:
    """Extract common tag info from batch EXIF read results into *info*.

    Handles IPTC caption detection, existing XMP date tags, extension
    mismatch detection, JPEG quality extraction, editor skip check, and
    blocked descriptions.  For matched files (``info.json_data`` is not
    None), blocked-description checks look at JSON first then EXIF; for
    orphans (``info.json_data`` is None) only EXIF is checked.
    """
    if tags.get('IPTC:Caption-Abstract'):
        info.has_iptc_caption = True

    existing = {t for t in CONDITIONAL_DATE_READ_TAGS if tags.get(t)}
    if existing:
        info.existing_xmp_dates = existing

    actual_type_ext = tags.get('File:FileTypeExtension')
    if actual_type_ext:
        actual = f'.{actual_type_ext.lower()}'
        source = info.source_path.suffix.lower()
        if actual != source and _is_real_ext_mismatch(source, actual):
            info.actual_ext = actual
            stats.ext_mismatches += 1
            label = "orphan " if info.json_data is None else ""
            logger.warning("Extension mismatch for %s%s: content is %s",
                           label, rel_fn(info.source_path), actual_type_ext)

    if jpeg_compress_quality is not None:
        if info.source_path.suffix.lower() in JPEG_EXTENSIONS:
            stats.jpeg_quality_checked += 1
            raw_q = tags.get('File:JPEGQualityEstimate')
            if raw_q is not None:
                try:
                    info.jpeg_quality = int(raw_q)
                except (ValueError, TypeError):
                    info.jpeg_quality = None
                    stats.jpeg_quality_unknown += 1
            else:
                stats.jpeg_quality_unknown += 1

    if editor_skip_patterns:
        _check_editor_skip(tags, info, editor_skip_patterns)

    if blocked_descriptions:
        if info.json_data:
            json_desc = info.json_data.get('description', '')
            if json_desc and json_desc in blocked_descriptions:
                info.clear_descriptions = True
            elif not json_desc:
                for desc_tag in DESC_READ_TAGS:
                    exif_desc = tags.get(desc_tag, '')
                    if exif_desc and str(exif_desc) in blocked_descriptions:
                        info.clear_descriptions = True
                        break
        else:
            for desc_tag in DESC_READ_TAGS:
                exif_desc = tags.get(desc_tag, '')
                if exif_desc and str(exif_desc) in blocked_descriptions:
                    info.clear_descriptions = True
                    break


def _run_strip_metadata_batch(et, file_paths: List[str],
                              infos: List[MediaFileInfo],
                              strip_read_tags: List[str]) -> None:
    """Batch-read strip metadata tags and flag files that have strippable metadata.

    Sets ``info.has_strip_metadata = True`` for each file whose tag result
    contains keys beyond the always-present SourceFile.
    """
    try:
        strip_results = et.get_tags(file_paths, strip_read_tags)
    except Exception:
        strip_results = [{} for _ in infos]
    for info, sr in zip(infos, strip_results):
        if len(sr) > 1:
            info.has_strip_metadata = True


def _build_date_params(info: MediaFileInfo) -> List[str]:
    """Build ExifTool date/timezone params based on ``info.write_strategy``.

    Returns an empty list if ``info.resolved_datetime`` is None.
    Dispatches on write strategy:
    - DIRECT: ``-alldates`` + OffsetTime tags + conditional date params
    - VIDEO_WITH_SIDECAR (QuickTime): UTC for QT tags, local+tz for
      UserData/XMP tags
    - PARTIAL_WITH_SIDECAR: XMP-only date tags with timezone
    """
    if not info.resolved_datetime:
        return []

    params: List[str] = []
    dt_str = info.resolved_datetime.strftime('%Y:%m:%d %H:%M:%S')
    tz_str = _format_tz_offset(info.resolved_datetime.tzinfo)

    is_qt_video = (info.write_strategy == WriteStrategy.VIDEO_WITH_SIDECAR
                   and info.source_path.suffix.lower() in QUICKTIME_VIDEO_EXTS)

    if is_qt_video:
        # QuickTime spec stores dates as UTC (no timezone field).
        utc_dt = info.resolved_datetime.astimezone(timezone.utc)
        utc_str = utc_dt.strftime('%Y:%m:%d %H:%M:%S')
        params.append(f'-QuickTime:CreateDate={utc_str}')
        params.append(f'-QuickTime:ModifyDate={utc_str}')
        # UserData and XMP tags carry local time with timezone suffix.
        local_with_tz = f'{dt_str}{tz_str}'
        params.append(f'-UserData:DateTimeOriginal={local_with_tz}')
        params.append(f'-XMP-exif:DateTimeOriginal={local_with_tz}')
        params.append(f'-XMP-xmp:CreateDate={local_with_tz}')
        params.append(f'-XMP-xmp:ModifyDate={local_with_tz}')
    elif info.write_strategy == WriteStrategy.PARTIAL_WITH_SIDECAR:
        # PNG/GIF lack EXIF; write XMP date tags explicitly so the
        # timezone is preserved in both the file and its sidecar.
        local_with_tz = f'{dt_str}{tz_str}'
        params.append(f'-XMP-exif:DateTimeOriginal={local_with_tz}')
        params.append(f'-XMP-xmp:CreateDate={local_with_tz}')
        params.append(f'-XMP-xmp:ModifyDate={local_with_tz}')
    else:
        params.append(f'-alldates={dt_str}')
        params.append(f'-EXIF:ExifIFD:OffsetTime={tz_str}')
        params.append(f'-EXIF:ExifIFD:OffsetTimeOriginal={tz_str}')
        params.append(f'-EXIF:ExifIFD:OffsetTimeDigitized={tz_str}')

    params.extend(_build_conditional_date_params(info, dt_str, tz_str))
    return params


def _build_description_params(info: MediaFileInfo,
                              stats: MergeStats) -> List[str]:
    """Build ExifTool params for description clearing or writing.

    Increments ``stats.descriptions_cleared`` when clearing.
    Returns a list of ExifTool CLI parameters (may include ``-E`` flag).
    """
    params: List[str] = []
    if info.clear_descriptions:
        params.append('-EXIF:UserComment=')
        params.append('-EXIF:ImageDescription=')
        params.append('-XMP-dc:Description=')
        if info.has_iptc_caption:
            params.append('-IPTC:Caption-Abstract=')
        stats.descriptions_cleared += 1
    elif info.description and info.description.strip():
        escaped, needs_E = _escape_description(info.description)
        if needs_E:
            params.append('-E')
        params.append(f'-XMP-dc:Description={escaped}')
        params.append(f'-EXIF:ImageDescription={escaped}')
        if info.has_iptc_caption:
            params.append(f'-IPTC:Caption-Abstract={escaped}')
    return params


def _build_gps_params(gps: Dict[str, float]) -> List[str]:
    """Build ExifTool GPS parameters from GPS dict."""
    lat = gps['latitude']
    lon = gps['longitude']
    alt = gps['altitude']

    params = [
        f'-EXIF:GPSLatitude={abs(lat)}',
        f'-EXIF:GPSLatitudeRef={"S" if lat < 0 else "N"}',
        f'-EXIF:GPSLongitude={abs(lon)}',
        f'-EXIF:GPSLongitudeRef={"W" if lon < 0 else "E"}',
        f'-EXIF:GPSAltitude={abs(alt)}',
        f'-EXIF:GPSAltitudeRef={"1" if alt < 0 else "0"}',
        f'-XMP:GPSLatitude={lat}',
        f'-XMP:GPSLongitude={lon}',
        f'-XMP:GPSAltitude={alt}',
    ]
    return params


def _build_conditional_date_params(info: MediaFileInfo, dt_str: str, tz_str: str) -> List[str]:
    """Build ExifTool params to update date tags that already exist in the source.

    Uses CONDITIONAL_DATE_TAGS mapping to translate read-tag names into the
    correct write parameters.
    """
    if not info.existing_xmp_dates:
        return []
    params: List[str] = []
    for read_tag in info.existing_xmp_dates:
        builder = CONDITIONAL_DATE_TAGS.get(read_tag)
        if builder:
            params.extend(builder(dt_str, tz_str))
    return params


def _build_sidecar_params(info: MediaFileInfo, gps: Optional[Dict[str, float]]) -> List[str]:
    """Build ExifTool params for creating an XMP sidecar."""
    params = ['-charset', 'filename=utf8']

    if info.new_title:
        params.append(f'-XMP:Title={Path(info.new_title).stem}')

    if info.resolved_datetime:
        tz_str = _format_tz_offset(info.resolved_datetime.tzinfo)
        dt_str = info.resolved_datetime.strftime('%Y:%m:%d %H:%M:%S') + tz_str
        params.append(f'-XMP:DateTimeOriginal={dt_str}')
        params.append(f'-XMP:CreateDate={dt_str}')
        params.append(f'-XMP:ModifyDate={dt_str}')

    if not info.clear_descriptions and info.description:
        desc = info.description
        if desc.strip():
            escaped, needs_E = _escape_description(desc)
            if needs_E:
                params.append('-E')
            params.append(f'-XMP-dc:Description={escaped}')

    if gps:
        params.append(f'-XMP:GPSLatitude={gps["latitude"]}')
        params.append(f'-XMP:GPSLongitude={gps["longitude"]}')
        params.append(f'-XMP:GPSAltitude={gps["altitude"]}')

    return params


def _escape_description(desc: str) -> tuple:
    """Escape newlines for ExifTool's -execute batch protocol.
    Normalizes CRLF and standalone CR to LF before escaping.
    Returns (escaped_desc, needs_E_flag)."""
    desc = desc.replace('\r\n', '\n').replace('\r', '\n')
    if '\n' not in desc:
        return desc, False
    escaped = desc.replace('&', '&amp;')
    escaped = escaped.replace('\n', '&#xa;')
    return escaped, True


# ---------------------------------------------------------------------------
# Core processing functions — used by both serial and parallel paths.
# Top-level (not methods) for ProcessPoolExecutor pickle compatibility.
# ---------------------------------------------------------------------------

def _execute_et(et, params: List[str]):
    """Execute ExifTool with UTF-8 encoded params."""
    et.execute(*[p.encode('utf-8') if isinstance(p, str) else p for p in params])


class _ext_mismatch_rename:
    """Context manager that temporarily renames a file to its actual extension.

    Some files have a mismatched extension (e.g. JPEG content with .DNG
    extension).  ExifTool refuses to write to these.  This context manager
    renames the file to the correct extension before the write, then renames
    it back afterwards.

    Usage::

        with _ext_mismatch_rename(output_path, info.actual_ext) as write_path:
            # write_path has the correct extension (or is unchanged if no mismatch)
            _execute_et(et, [..., str(write_path)])
    """
    def __init__(self, path: Path, actual_ext: Optional[str]):
        self.original = path
        self.actual_ext = actual_ext
        if actual_ext and path.suffix.lower() != actual_ext:
            self.temp = path.with_suffix(actual_ext)
        else:
            self.temp = None

    def __enter__(self) -> Path:
        if self.temp and self.original.exists():
            self.original.rename(self.temp)
            return self.temp
        return self.original

    def __exit__(self, *exc):
        if self.temp and self.temp.exists():
            self.temp.rename(self.original)
        return False


def _log_jpeg_skip_counters(info: MediaFileInfo, stats: MergeStats,
                            logger: logging.Logger, label: str = "") -> None:
    """Check and log JPEG compression skips due to timerange or editor.

    Increments ``stats.jpeg_compress_skipped_timerange`` or
    ``stats.jpeg_compress_skipped_editor`` when the file would have been
    compressed but is excluded.  *label* (e.g. ``"orphan, "``) is
    inserted into log messages for context.
    """
    would_compress = (info.jpeg_compress_quality is not None
                      and info.source_path.suffix.lower() in JPEG_EXTENSIONS
                      and (info.jpeg_quality is None
                           or info.jpeg_quality > info.jpeg_compress_quality))
    if would_compress and info.jpeg_skip_timerange:
        stats.jpeg_compress_skipped_timerange += 1
        logger.info("SKIP-TIME    %s  (%swithin excluded time range, skipping compression)",
                     info.source_path.name, label)
    elif would_compress and info.jpeg_skip_editor:
        stats.jpeg_compress_skipped_editor += 1
        logger.info("SKIP-EDITOR  %s  (%sexported from known editor, skipping compression)",
                     info.source_path.name, label)


def _compress_and_write_jpeg(info: MediaFileInfo, tag_params: List[str],
                             stats: MergeStats, logger: logging.Logger,
                             label: str = "") -> bool:
    """Compress a JPEG with Pillow and write it with metadata via exiftool.

    Handles Pillow compression, size guard (falls back to original bytes
    if compression doesn't reduce size), ``_write_compressed_jpeg_with_metadata``
    call, logging, and stats counters.  *label* (e.g. ``"orphan "``) is
    inserted into log messages.  Returns True on success, False on error.
    """
    try:
        jpeg_bytes = _compress_jpeg_to_bytes(info.source_path,
                                             info.jpeg_compress_quality)
    except Exception as e:
        logger.error("Pillow compression failed for %s%s: %s",
                     label, info.source_path, e)
        stats.errors += 1
        return False

    original_size = info.source_path.stat().st_size
    if len(jpeg_bytes) >= original_size:
        logger.info("SKIP-COMPRESS  %s  (%scompressed %d bytes >= original %d bytes, using original)",
                    info.source_path, label, len(jpeg_bytes), original_size)
        jpeg_bytes = info.source_path.read_bytes()
        stats.jpeg_compress_skipped_larger += 1
        compressed = False
    else:
        compressed = True

    if not _write_compressed_jpeg_with_metadata(
        jpeg_bytes, info.source_path, info.output_path, tag_params, logger
    ):
        stats.errors += 1
        return False

    if compressed:
        q_str = f'{info.jpeg_quality}%' if info.jpeg_quality is not None else 'unknown'
        logger.info("COMPRESS  %s  (%swas ~%s -> %d%%, %d bytes -> %d bytes)",
                    info.source_path.name, label, q_str, info.jpeg_compress_quality,
                    original_size, len(jpeg_bytes))
        stats.jpeg_compressed += 1

    return True


def _needs_jpeg_compression(info: MediaFileInfo) -> bool:
    """Return True if this file is a JPEG that should be recompressed.

    Compression is triggered when all of these are true:
    - JPEG compression is enabled (jpeg_compress_quality is set)
    - The file is NOT excluded by a time-range skip
    - The source file was NOT exported from a skipped editor
    - The source file has a JPEG extension
    - The estimated quality exceeds the target threshold, OR the quality
      could not be determined (conservative — recompress to be safe)
    """
    if info.jpeg_compress_quality is None:
        return False
    if info.jpeg_skip_timerange:
        return False
    if info.jpeg_skip_editor:
        return False
    if info.source_path.suffix.lower() not in JPEG_EXTENSIONS:
        return False
    if info.jpeg_quality is None:
        return True  # unknown quality — recompress to be safe
    return info.jpeg_quality > info.jpeg_compress_quality


def _compress_jpeg_to_bytes(source_path: Path, quality: int) -> bytes:
    """Compress a JPEG with Pillow and return the raw bytes (no metadata).

    The image is saved to an in-memory buffer at the given quality level
    with optimisation enabled.  No EXIF/XMP/IPTC data is transferred —
    metadata is handled separately by ExifTool.
    """
    img = Image.open(source_path)
    buf = BytesIO()
    img.save(buf, format='JPEG', quality=quality, optimize=True)
    img.close()
    return buf.getvalue()


def _write_compressed_jpeg_with_metadata(
    jpeg_bytes: bytes, source_path: Path, output_path: Path,
    tag_params: List[str], logger: logging.Logger,
) -> bool:
    """Pipe compressed JPEG bytes through exiftool to write metadata.

    Runs a standalone exiftool subprocess that:
    1. Reads the compressed JPEG from stdin (``-``)
    2. Copies ALL metadata from the original source via ``-TagsFromFile``
    3. Applies tag modifications (dates, descriptions, GPS, etc.)
    4. Writes the result to *output_path*

    Arguments are passed via a UTF-8 encoded argfile (``-@``) rather than
    on the command line, because on Windows the command line is decoded
    using the system code page (e.g. cp1252) which corrupts non-Latin
    characters such as CJK ideographs in descriptions.

    This avoids writing the compressed image to disk as an intermediate
    step.  The piping approach (stdin) works on both Windows and Linux.

    Returns True on success, False on failure.
    """
    # Build the argument list — one argument per line in the argfile.
    args = [
        '-n',                             # numeric tag values (GPSAltitudeRef etc.)
        '-charset', 'utf8',
        '-charset', 'filename=utf8',
        '-TagsFromFile', str(source_path),
        '-All:All',                       # copy every tag group
    ]
    args.extend(tag_params)
    args.extend(['-o', str(output_path), '-'])

    # Write arguments to a temporary UTF-8 argfile so ExifTool reads them
    # with the correct encoding regardless of the Windows system code page.
    argfile_fd, argfile_path = tempfile.mkstemp(suffix='.args', prefix='et_')
    try:
        with os.fdopen(argfile_fd, 'w', encoding='utf-8') as f:
            for arg in args:
                f.write(arg + '\n')

        cmd = ['exiftool', '-@', argfile_path]

        try:
            result = subprocess.run(
                cmd,
                input=jpeg_bytes,
                capture_output=True,
                timeout=120,
            )
        except subprocess.TimeoutExpired:
            logger.error("ExifTool timed out for compressed %s", source_path)
            return False
        except Exception as e:
            logger.error("ExifTool subprocess failed for %s: %s", source_path, e)
            return False
    finally:
        try:
            os.unlink(argfile_path)
        except OSError:
            pass

    if not output_path.exists():
        stderr_msg = result.stderr.decode('utf-8', errors='replace').strip()
        logger.error("ExifTool compress+write failed for %s (rc=%d): %s",
                     source_path, result.returncode, stderr_msg)
        return False

    if result.returncode != 0:
        stderr_msg = result.stderr.decode('utf-8', errors='replace').strip()
        logger.debug("ExifTool warnings for compressed %s (rc=%d): %s",
                     source_path, result.returncode, stderr_msg)

    return True


def _do_process_matched(et, info: MediaFileInfo, stats: MergeStats,
                        logger: logging.Logger):
    """Core logic for processing a matched media file."""
    if info.output_path.exists():
        logger.warning("Output file already exists, skipping: %s", info.output_path)
        stats.skipped_existing += 1
        return

    info.output_path.parent.mkdir(parents=True, exist_ok=True)

    _log_jpeg_skip_counters(info, stats, logger)

    # Non-QuickTime video containers (AVI, MKV, WebM) cannot have tags
    # written directly — ExifTool does not support writing to these formats.
    # Just copy the file; all metadata lives in the XMP sidecar.
    is_qt_video = (info.write_strategy == WriteStrategy.VIDEO_WITH_SIDECAR
                   and info.source_path.suffix.lower() in QUICKTIME_VIDEO_EXTS)
    is_non_qt_video = (info.write_strategy == WriteStrategy.VIDEO_WITH_SIDECAR
                       and not is_qt_video)

    if is_non_qt_video:
        try:
            shutil.copy2(str(info.source_path), str(info.output_path))
        except Exception as e:
            logger.error("Failed to copy %s: %s", info.source_path, e)
            stats.errors += 1
            return
        # GPS and description stats are deferred to after sidecar creation
        # so they are only counted when the sidecar is successfully written.
    elif _needs_jpeg_compression(info):
        # ----- Compressed JPEG path -----
        tag_params = _build_date_params(info)
        tag_params.extend(_build_description_params(info, stats))

        if info.gps:
            tag_params.extend(_build_gps_params(info.gps))
            stats.gps_written += 1

        tag_params.append('-IPTCDigest=new')

        if not _compress_and_write_jpeg(info, tag_params, stats, logger):
            return
    else:
        params = ['-charset', 'filename=utf8']
        params.extend(_build_date_params(info))
        params.extend(_build_description_params(info, stats))

        if info.gps:
            params.extend(_build_gps_params(info.gps))
            stats.gps_written += 1

        # Recompute IPTCDigest so it stays in sync after any IPTC changes.
        # Harmless no-op when no IPTC data exists.
        params.append('-IPTCDigest=new')

        # Use -o to copy source to output with metadata in one step.
        # If the file has a content/extension mismatch, use a temp path with
        # the correct extension so ExifTool can process it, then rename back.
        if info.actual_ext:
            temp_output = info.output_path.with_suffix(info.actual_ext)
            temp_source = info.source_path.parent / (info.source_path.stem + info.actual_ext)
        else:
            temp_output = None
            temp_source = None

        write_output = temp_output or info.output_path
        params.append('-o')
        params.append(str(write_output))

        # For -o, ExifTool checks the *source* file extension, so we need to
        # temporarily rename the source when there's a mismatch.
        if temp_source:
            try:
                info.source_path.rename(temp_source)
            except OSError:
                temp_source = None  # rename failed; proceed with original

        params.append(str(temp_source or info.source_path))

        try:
            _execute_et(et, params)
        except Exception as e:
            if write_output.exists():
                logger.debug("ExifTool warnings for %s: %s (output created successfully)",
                             info.source_path, e)
            else:
                # Genuine failure: output not created. Fallback to copy + in-place write.
                logger.warning("ExifTool -o failed for %s: %s, falling back to copy+write",
                               info.source_path, e)
                try:
                    shutil.copy2(str(temp_source or info.source_path), str(write_output))
                    fallback_params = ['-charset', 'filename=utf8', '-overwrite_original']
                    fallback_params.extend(params[2:-3])  # tag params only
                    fallback_params.append(str(write_output))
                    _execute_et(et, fallback_params)
                except Exception as e2:
                    if not write_output.exists():
                        logger.error("Failed to process %s: %s", info.source_path, e2)
                        stats.errors += 1
                        # Restore source rename before returning
                        if temp_source and temp_source.exists():
                            temp_source.rename(info.source_path)
                        return
        finally:
            # Restore source file name
            if temp_source and temp_source.exists():
                temp_source.rename(info.source_path)
            # Rename output to the intended path
            if temp_output and temp_output.exists() and not info.output_path.exists():
                temp_output.rename(info.output_path)

    stats.written += 1

    if info.sidecar_path:
        _sidecar_before = stats.sidecars_created
        _do_create_sidecar(et, info, stats, logger)
        # For non-QT video, GPS and descriptions are sidecar-only;
        # count them only when the sidecar was successfully created.
        if is_non_qt_video and stats.sidecars_created > _sidecar_before:
            if info.gps:
                stats.gps_written += 1
            if info.clear_descriptions:
                stats.descriptions_cleared += 1

    _do_strip_metadata(et, info, stats, logger)
    _do_set_filesystem_timestamps(et, info, logger)


def _do_process_orphan(et, info: MediaFileInfo, stats: MergeStats,
                       logger: logging.Logger):
    """Core logic for processing an orphan media file."""
    if info.output_path.exists():
        logger.warning("Output file already exists, skipping orphan: %s", info.output_path)
        stats.skipped_existing += 1
        return

    info.output_path.parent.mkdir(parents=True, exist_ok=True)

    _log_jpeg_skip_counters(info, stats, logger, label="orphan, ")

    if _needs_jpeg_compression(info):
        # ----- Compressed JPEG orphan path -----
        tag_params = _build_date_params(info)
        tag_params.extend(_build_description_params(info, stats))

        if tag_params:
            tag_params.append('-IPTCDigest=new')

        if not _compress_and_write_jpeg(info, tag_params, stats, logger, label="orphan "):
            return
        stats.written += 1

        if info.sidecar_path:
            _do_create_sidecar(et, info, stats, logger)
        _do_strip_metadata(et, info, stats, logger)
        _do_set_filesystem_timestamps(et, info, logger)
        return

    try:
        shutil.copy2(str(info.source_path), str(info.output_path))
        stats.written += 1
    except Exception as e:
        logger.error("Failed to copy orphan %s: %s", info.source_path, e)
        stats.errors += 1
        return

    # Build params for in-place updates (descriptions + dates).
    update_params = ['-charset', 'filename=utf8', '-overwrite_original']
    update_params.extend(_build_description_params(info, stats))

    # Fill in any missing date tags and add timezone offsets.
    # Non-QuickTime video containers cannot have tags written in-place.
    is_non_qt_video = (info.write_strategy == WriteStrategy.VIDEO_WITH_SIDECAR
                       and info.source_path.suffix.lower() not in QUICKTIME_VIDEO_EXTS)

    if not is_non_qt_video:
        update_params.extend(_build_date_params(info))

    # Only call ExifTool if there are tag params beyond the base three
    # (charset, filename, overwrite_original).
    if len(update_params) > 3:
        # Recompute IPTCDigest so it stays in sync after any IPTC changes.
        update_params.append('-IPTCDigest=new')
        # Temporarily rename output file if extension/content mismatch.
        with _ext_mismatch_rename(info.output_path, info.actual_ext) as write_path:
            update_params.append(str(write_path))
            try:
                _execute_et(et, update_params)
            except Exception as e:
                if write_path.exists():
                    logger.debug("ExifTool warnings for orphan %s: %s", info.source_path, e)
                else:
                    logger.warning("Failed to update orphan %s: %s", info.source_path, e)

    if info.sidecar_path:
        _do_create_sidecar(et, info, stats, logger)
    _do_strip_metadata(et, info, stats, logger)
    _do_set_filesystem_timestamps(et, info, logger)


def _do_create_sidecar(et, info: MediaFileInfo,
                       stats: MergeStats, logger: logging.Logger):
    """Core logic for creating an XMP sidecar file."""
    if info.sidecar_path is None:
        return

    sidecar_params = _build_sidecar_params(info, info.gps)
    sidecar_params.append('-o')
    sidecar_params.append(str(info.sidecar_path))
    sidecar_params.append(str(info.output_path))

    try:
        _execute_et(et, sidecar_params)
        stats.sidecars_created += 1
        logger.info("Created XMP sidecar for %s: %s", info.source_path, info.sidecar_path.name)
    except Exception as e:
        logger.warning("Failed to create XMP sidecar for %s: %s", info.source_path, e)
        return

    # ExifTool's -o copies existing metadata from the source file before
    # applying overrides.  Pre-existing tags — including non-XMP dates
    # (e.g. Nikon maker-note CreateDate) that ExifTool maps into XMP — win
    # over the params we pass, so a second in-place pass is needed to force
    # the correct values with timezone.
    if info.resolved_datetime:
        dt_str = info.resolved_datetime.strftime('%Y:%m:%d %H:%M:%S')
        tz_str = _format_tz_offset(info.resolved_datetime.tzinfo)
        local_with_tz = f'{dt_str}{tz_str}'
        # Always force-write the three core sidecar date tags so that any
        # values injected by -o from non-XMP sources are corrected.
        fixup_params = [
            '-charset', 'filename=utf8', '-overwrite_original',
            f'-XMP-xmp:CreateDate={local_with_tz}',
            f'-XMP-xmp:ModifyDate={local_with_tz}',
            f'-XMP-exif:DateTimeOriginal={local_with_tz}',
        ]
        # Also fix any additional conditional XMP date tags that existed
        # in the source file (e.g. XMP-photoshop:DateCreated).
        fixup_params.extend(_build_conditional_date_params(info, dt_str, tz_str))
        fixup_params.append(str(info.sidecar_path))
        try:
            _execute_et(et, fixup_params)
        except Exception as e:
            logger.debug("Sidecar fixup warnings for %s: %s", info.sidecar_path.name, e)


def _do_set_filesystem_timestamps(et, info: MediaFileInfo, logger: logging.Logger):
    """Core logic for setting filesystem timestamps."""
    if info.resolved_datetime is None:
        return

    fb_tz = info.fallback_tz or timezone.utc
    local_dt = info.resolved_datetime.astimezone(fb_tz)
    tz_str = _format_tz_offset(fb_tz)
    dt_str = local_dt.strftime('%Y:%m:%d %H:%M:%S') + tz_str

    files_to_update = [info.output_path]
    if info.sidecar_path and info.sidecar_path.exists():
        files_to_update.append(info.sidecar_path)

    for file_path in files_to_update:
        try:
            ts_params = [
                '-overwrite_original',
                f'-FileCreateDate={dt_str}',
                f'-FileModifyDate={dt_str}',
                str(file_path),
            ]
            _execute_et(et, ts_params)
        except Exception as e:
            logger.warning("Failed to set filesystem timestamps for %s: %s", info.source_path, e)


def _do_strip_metadata(et, info: MediaFileInfo, stats: MergeStats,
                       logger: logging.Logger):
    """Remove unwanted metadata groups from the output file.

    Only runs when info.strip_metadata_params is set (non-None, non-empty)
    **and** the source file was found to contain metadata targeted by those
    params during the batch EXIF read in _resolve_dates_and_paths (stored
    as info.has_strip_metadata).  Skips files with nothing to strip,
    avoiding unnecessary ExifTool calls and log noise.

    Operates on the output file in-place after all other writes are complete.
    Video containers that ExifTool cannot write to (AVI, MKV, WebM) are
    skipped — their metadata lives in the XMP sidecar which is built fresh.
    """
    if not info.strip_metadata_params:
        return
    if not info.has_strip_metadata:
        return
    if not info.output_path or not info.output_path.exists():
        return

    # Non-QuickTime video containers are copy-only; ExifTool cannot modify
    # them in-place, so there's nothing to strip from the file itself.
    is_non_qt_video = (info.write_strategy == WriteStrategy.VIDEO_WITH_SIDECAR
                       and info.source_path.suffix.lower() not in QUICKTIME_VIDEO_EXTS)
    if is_non_qt_video:
        return

    params = ['-charset', 'filename=utf8', '-overwrite_original']
    params.extend(info.strip_metadata_params)
    params.append(str(info.output_path))
    try:
        _execute_et(et, params)
        stats.metadata_stripped += 1
        logger.info("Stripped metadata from %s", info.output_path.name)
    except Exception as e:
        logger.warning("Metadata strip warnings for %s: %s", info.output_path.name, e)

def _setup_worker_logging() -> logging.Logger:
    """Configure and return a logger for the current worker process."""
    logger = logging.getLogger(f'Worker-{os.getpid()}')
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S'))
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    return logger


def _process_chunk(chunk: List[MediaFileInfo]) -> MergeStats:
    """Process a list of MediaFileInfo items in a worker process.

    Each worker opens its own ExifTool instance.  Returns a partial
    MergeStats with only the processing counters populated.
    """
    logger = _setup_worker_logging()
    stats = MergeStats()
    et_helper = exiftool.ExifToolHelper()
    et = et_helper.__enter__()
    try:
        for info in chunk:
            try:
                if info.is_orphan:
                    _do_process_orphan(et, info, stats, logger)
                else:
                    _do_process_matched(et, info, stats, logger)
            except Exception as e:
                logger.error("Failed to process %s: %s", info.source_path, e)
                stats.errors += 1
    finally:
        et_helper.__exit__(None, None, None)
    return stats


class PhotosExportMerger(AbstractMediaMerger):
    def __init__(self, input_dir: str, output_dir: str, dry_run: bool = False,
                 blocked_descriptions: Optional[List[str]] = None,
                 num_workers: int = 1,
                 metadata_strip_params: Optional[List[str]] = None,
                 tz_overrides: Optional[List[TimezoneOverride]] = None,
                 fallback_tz: Optional[timezone] = None,
                 jpeg_compress_quality: Optional[int] = None,
                 editor_skip_patterns: Optional[List[Dict[str, List[str]]]] = None,
                 jpeg_compress_skip_timeranges: Optional[List[JpegSkipTimerange]] = None):
        super().__init__(input_dir, output_dir, dry_run, blocked_descriptions,
                         num_workers=num_workers,
                         metadata_strip_params=metadata_strip_params,
                         tz_overrides=tz_overrides,
                         fallback_tz=fallback_tz,
                         jpeg_compress_quality=jpeg_compress_quality,
                         editor_skip_patterns=editor_skip_patterns,
                         jpeg_compress_skip_timeranges=jpeg_compress_skip_timeranges)

    def _open_writer(self) -> None:
        self._et_helper = exiftool.ExifToolHelper()
        self._et = self._et_helper.__enter__()

    def _close_writer(self) -> None:
        if hasattr(self, '_et_helper'):
            self._et_helper.__exit__(None, None, None)

    def _validate_directories(self):
        if not self.input_path.exists():
            raise FileNotFoundError(f"Input directory does not exist: {self.input_path}")
        if not self.input_path.is_dir():
            raise NotADirectoryError(f"Input path is not a directory: {self.input_path}")
        try:
            self.output_path.relative_to(self.input_path)
            raise ValueError(f"Output directory must not be inside input directory: {self.output_path}")
        except ValueError as e:
            if "must not be inside" in str(e):
                raise
            # relative_to raised ValueError -> output is NOT inside input, which is correct
        if not self.dry_run:
            self.output_path.mkdir(parents=True, exist_ok=True)

    def _scan_files(self):
        self.logger.info("Scanning directory tree: %s", self.input_path)
        non_json_by_dir = defaultdict(SortedSet)
        json_by_dir = defaultdict(list)

        for f in self.input_path.rglob('*'):
            if not f.is_file():
                continue
            # Limit to 2 directory levels deep
            try:
                rel = f.relative_to(self.input_path)
            except ValueError:
                continue
            if len(rel.parts) - 1 > 2:  # parts includes the filename
                continue

            if f.suffix.lower() == '.json':
                json_by_dir[f.parent].append(f)
            else:
                non_json_by_dir[f.parent].add(f.name)

        total_json = sum(len(v) for v in json_by_dir.values())
        total_media = sum(len(v) for v in non_json_by_dir.values())
        self.logger.info("Found %d JSON files and %d non-JSON files in %d directories",
                         total_json, total_media,
                         len(set(json_by_dir.keys()) | set(non_json_by_dir.keys())))
        return non_json_by_dir, json_by_dir

    def _match_metadata_to_media(self, media_by_dir, metadata_by_dir, stats) -> tuple:
        media_files: List[MediaFileInfo] = []
        referenced_by_dir: Dict[Path, set] = defaultdict(set)

        for dir_path, json_files in metadata_by_dir.items():
            dir_non_json = media_by_dir.get(dir_path, SortedSet())

            for json_path in json_files:
                try:
                    with open(json_path, 'r', encoding='utf-8') as f:
                        json_data = json.load(f)
                except (json.JSONDecodeError, IOError, UnicodeDecodeError) as e:
                    self.logger.warning("Failed to parse JSON %s: %s", self._rel(json_path), e)
                    stats.skipped_json += 1
                    continue

                matching_filename, new_title = JsonFileFinder(
                    str(json_path),
                    json_data=json_data,
                    dir_files=dir_non_json
                )

                if matching_filename is None:
                    self.logger.warning("No matching media file for JSON: %s (title: %s)",
                                        self._rel(json_path), new_title)
                    stats.skipped_json += 1
                    continue

                source_path = dir_path / matching_filename
                ext = source_path.suffix.lower()
                strategy = _get_write_strategy(ext)

                if strategy is None:
                    self.logger.warning("Unsupported file type %s for: %s", ext, self._rel(source_path))
                    stats.skipped_json += 1
                    continue

                info = MediaFileInfo(
                    source_path=source_path,
                    filename=matching_filename,
                    json_data=json_data,
                    new_title=new_title,
                    write_strategy=strategy,
                    is_orphan=False,
                )
                media_files.append(info)
                referenced_by_dir[dir_path].add(matching_filename)
                stats.matched += 1

        return media_files, referenced_by_dir

    def _identify_orphans(self, non_json_by_dir, referenced_by_dir) -> List[MediaFileInfo]:
        orphans = []
        for dir_path, filenames in non_json_by_dir.items():
            referenced = referenced_by_dir.get(dir_path, set())
            for filename in filenames:
                if filename not in referenced:
                    source_path = dir_path / filename
                    ext = source_path.suffix.lower()
                    strategy = _get_write_strategy(ext)
                    if strategy is None:
                        self.logger.warning("Skipping unsupported file type %s: %s", source_path.suffix.lower() or '(no extension)', self._rel(source_path))
                        continue
                    info = MediaFileInfo(
                        source_path=source_path,
                        filename=filename,
                        new_title=filename,
                        write_strategy=strategy,
                        is_orphan=True,
                    )
                    orphans.append(info)
                    self.logger.info("Orphan file (no matching JSON): %s", self._rel(source_path))
        return orphans

    def _resolve_dates_and_paths(self, media_files: List[MediaFileInfo], stats: MergeStats):
        # Group files by directory for batch EXIF reads
        matched_by_dir: Dict[Path, List[MediaFileInfo]] = defaultdict(list)
        orphans_by_dir: Dict[Path, List[MediaFileInfo]] = defaultdict(list)

        for info in media_files:
            if info.is_orphan:
                orphans_by_dir[info.source_path.parent].append(info)
            else:
                matched_by_dir[info.source_path.parent].append(info)

        # Derive read-tag names from strip params (e.g. "-XMP-GCamera:All="
        # → "XMP-GCamera:All") so the batch read can detect whether each
        # source file actually contains metadata that needs stripping.
        strip_read_tags: List[str] = []
        if self.metadata_strip_params:
            strip_read_tags = [p.lstrip('-').rstrip('=')
                               for p in self.metadata_strip_params]

        # Resolve timezone for matched files (batch read per directory)
        for dir_path, infos in matched_by_dir.items():
            file_paths = [str(info.source_path) for info in infos]
            tz_tags = ['EXIF:OffsetTimeOriginal', 'EXIF:OffsetTime']
            read_tags = _build_batch_read_tags(tz_tags, self.blocked_descriptions,
                                               self.jpeg_compress_quality,
                                               self.editor_skip_patterns)

            try:
                tag_results = self._et.get_tags(file_paths, read_tags)
            except Exception as e:
                self.logger.warning("Failed to batch-read timezone from %s: %s", self._rel(dir_path), e)
                tag_results = [{} for _ in infos]

            for info, tags in zip(infos, tag_results):
                _extract_common_tags(info, tags, stats,
                                     self.jpeg_compress_quality,
                                     self.editor_skip_patterns,
                                     self.blocked_descriptions,
                                     self.logger, self._rel)

                epoch_str = None
                if info.json_data:
                    pt = info.json_data.get('photoTakenTime')
                    if pt:
                        epoch_str = pt.get('timestamp')

                if epoch_str is None:
                    info.error = "No photoTakenTime in JSON"
                    self.logger.warning("No photoTakenTime for %s", self._rel(info.source_path))
                    continue

                epoch = int(epoch_str)

                # Determine timezone from media file EXIF
                tz = None
                for tag_key in tz_tags:
                    offset_val = tags.get(tag_key)
                    if offset_val:
                        tz = _parse_tz_offset(str(offset_val))
                        if tz:
                            break

                if tz is None:
                    utc_dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
                    override_tz = _find_tz_override(utc_dt, self.tz_overrides)
                    if override_tz is not None:
                        tz = override_tz
                        self.logger.info("No timezone in EXIF for %s, using override %s",
                                         self._rel(info.source_path), _format_tz_offset(tz))
                    else:
                        tz = self.fallback_tz
                        self.logger.info("No timezone in EXIF for %s, using %s fallback",
                                         self._rel(info.source_path), _format_tz_offset(tz))

                utc_dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
                local_dt = utc_dt.astimezone(tz)
                info.resolved_datetime = local_dt
                info.date_source = 'json_photoTakenTime'
                info.year = local_dt.strftime('%Y')
                info.month = local_dt.strftime('%m')

                # Check if file falls within an excluded time range.
                if self.jpeg_compress_skip_timeranges:
                    _check_jpeg_skip_timerange(info, self.jpeg_compress_skip_timeranges)

            # Separate batch read for strip-metadata detection.
            # ExifTool normalises group names in returned keys (e.g.
            # XMP-GCamera:All → XMP:SpecialTypeID), so we cannot match
            # returned keys against strip param names.  Instead we query
            # only the strip tags and flag any file whose result contains
            # keys beyond the always-present SourceFile.
            if strip_read_tags:
                _run_strip_metadata_batch(self._et, file_paths, infos, strip_read_tags)

        # Resolve dates for orphan files (batch read per directory)
        for dir_path, infos in orphans_by_dir.items():
            file_paths = [str(info.source_path) for info in infos]
            read_tags = _build_batch_read_tags(DATE_TAGS_PRIORITY, self.blocked_descriptions,
                                               self.jpeg_compress_quality,
                                               self.editor_skip_patterns)

            try:
                tag_results = self._et.get_tags(file_paths, read_tags)
            except Exception as e:
                self.logger.warning("Failed to batch-read dates from %s: %s", self._rel(dir_path), e)
                tag_results = [{} for _ in infos]

            for info, tags in zip(infos, tag_results):
                _extract_common_tags(info, tags, stats,
                                     self.jpeg_compress_quality,
                                     self.editor_skip_patterns,
                                     self.blocked_descriptions,
                                     self.logger, self._rel)

                resolved_dt = None
                date_source = None

                for tag_key in DATE_TAGS_PRIORITY:
                    val = tags.get(tag_key)
                    if val and isinstance(val, str) and val.strip():
                        try:
                            # EXIF dates are typically "YYYY:MM:DD HH:MM:SS"
                            parsed = datetime.strptime(val.strip()[:19], '%Y:%m:%d %H:%M:%S')
                            # Check tz overrides: use the naive date as a UTC
                            # approximation for range matching.
                            approx_utc = parsed.replace(tzinfo=timezone.utc)
                            override_tz = _find_tz_override(approx_utc, self.tz_overrides)
                            resolved_tz = override_tz if override_tz is not None else self.fallback_tz
                            resolved_dt = parsed.replace(tzinfo=resolved_tz)
                            date_source = tag_key
                            stats.date_from_exif += 1
                            break
                        except ValueError:
                            continue

                if resolved_dt is None:
                    # Last resort: file creation date
                    try:
                        ctime = info.source_path.stat().st_ctime
                        utc_ctime = datetime.fromtimestamp(ctime, tz=timezone.utc)
                        override_tz = _find_tz_override(utc_ctime, self.tz_overrides)
                        resolved_tz = override_tz if override_tz is not None else self.fallback_tz
                        resolved_dt = datetime.fromtimestamp(ctime, tz=resolved_tz)
                        date_source = 'file_creation_date'
                        stats.date_from_filesystem += 1
                        self.logger.warning("Using file creation date for orphan: %s", self._rel(info.source_path))
                    except OSError as e:
                        self.logger.error("Cannot stat file %s: %s", self._rel(info.source_path), e)

                if resolved_dt:
                    info.resolved_datetime = resolved_dt
                    info.date_source = date_source
                    info.year = resolved_dt.strftime('%Y')
                    info.month = resolved_dt.strftime('%m')

                    # Check if file falls within an excluded time range.
                    if self.jpeg_compress_skip_timeranges:
                        _check_jpeg_skip_timerange(info, self.jpeg_compress_skip_timeranges)

            # Separate batch read for strip-metadata detection (see matched
            # files above for rationale).
            if strip_read_tags:
                _run_strip_metadata_batch(self._et, file_paths, infos, strip_read_tags)

        # Build output paths
        for info in media_files:
            if info.year is None or info.month is None:
                info.year = 'unknown'
                info.month = 'unknown'

            title = info.new_title or info.filename
            # Ensure title has correct extension (always lowercase)
            title_ext = Path(title).suffix.lower()
            source_ext = info.source_path.suffix.lower()
            if title_ext != source_ext:
                title = Path(title).stem + source_ext
            elif Path(title).suffix != title_ext:
                title = Path(title).stem + title_ext

            info.new_title = title
            info.output_path = self.output_path / info.year / info.month / title

            if info.write_strategy in (WriteStrategy.PARTIAL_WITH_SIDECAR, WriteStrategy.VIDEO_WITH_SIDECAR):
                info.sidecar_path = info.output_path.parent / (title + '.xmp')

        # Extract fields needed for processing and clear json_data to reduce
        # serialisation payload when dispatching to worker processes.
        for info in media_files:
            if info.json_data:
                info.description = info.json_data.get('description', '')
                info.gps = _resolve_gps(info.json_data)
                info.json_data = None
            info.strip_metadata_params = self.metadata_strip_params
            info.fallback_tz = self.fallback_tz
            info.jpeg_compress_quality = self.jpeg_compress_quality

    def _process_matched(self, info: MediaFileInfo, stats: MergeStats):
        if self.dry_run:
            self._log_dry_run(info)
            return
        _do_process_matched(self._et, info, stats, self.logger)

    def _process_orphan(self, info: MediaFileInfo, stats: MergeStats):
        if self.dry_run:
            self._log_dry_run(info)
            return
        _do_process_orphan(self._et, info, stats, self.logger)

    def _process_files_parallel(self, media_files: List[MediaFileInfo], stats: MergeStats) -> None:
        """Split media_files across worker processes, each with its own ExifTool."""
        num_workers = min(self.num_workers, len(media_files))
        self.logger.info("Processing %d files across %d workers", len(media_files), num_workers)

        # Round-robin distribute files to workers for balanced chunk sizes
        chunks: List[List[MediaFileInfo]] = [[] for _ in range(num_workers)]
        for i, info in enumerate(media_files):
            chunks[i % num_workers].append(info)

        with ProcessPoolExecutor(max_workers=num_workers) as pool:
            futures = {
                pool.submit(_process_chunk, chunk): i
                for i, chunk in enumerate(chunks)
            }
            for future in as_completed(futures):
                worker_idx = futures[future]
                try:
                    partial_stats = future.result()
                    stats.merge(partial_stats)
                except Exception as e:
                    self.logger.error(
                        "Worker %d failed: %s (counting all %d files in chunk as errors)",
                        worker_idx, e, len(chunks[worker_idx]))
                    stats.errors += len(chunks[worker_idx])


if __name__ == '__main__':
    import argparse

    _profiles = ', '.join(METADATA_STRIP_PROFILES.keys())

    parser = argparse.ArgumentParser(
        prog='PhotosExportMerger.py',
        description=(
            'Merge JSON metadata from Photos Takeout exports into '
            'image/video EXIF properties using ExifTool.  Copies files into '
            'a date-organized output directory (YYYY/MM/filename).'
        ),
        epilog=(
            'examples:\n'
            '  # Basic merge\n'
            '  python PhotosExportMerger.py input/ output/\n'
            '\n'
            '  # Dry run — preview without writing\n'
            '  python PhotosExportMerger.py input/ output/ --dry-run\n'
            '\n'
            '  # Use 4 workers and strip Google camera metadata\n'
            '  python PhotosExportMerger.py input/ output/ --workers 4 --strip-metadata google\n'
            '\n'
            '  # Recompress JPEGs above 80% quality\n'
            '  python PhotosExportMerger.py input/ output/ --jpeg-quality-threshold 80\n'
            '\n'
            '  # Recompress JPEGs but skip Lightroom and Darktable exports\n'
            '  python PhotosExportMerger.py input/ output/ --jpeg-quality-threshold 80 \\\n'
            '    --jpeg-quality-skip-editor lightroom --jpeg-quality-skip-editor darktable\n'
            '\n'
            '  # Set fallback timezone and override for a trip\n'
            '  python PhotosExportMerger.py input/ output/ \\\n'
            '    --tz-fallback "+02:00" \\\n'
            '    --tz-override "2019-11-20 02:00:00,2019-11-22 17:00:50,+05:30"\n'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('input_dir',
                        help='Root of the Photos Takeout export.')
    parser.add_argument('output_dir',
                        help='Destination directory (created if needed). '
                             'Must not be inside input_dir.')
    parser.add_argument('--dry-run', action='store_true',
                        help='Simulate the merge without writing any files.')
    parser.add_argument('--workers', type=int, default=os.cpu_count() or 1,
                        metavar='N',
                        help='Number of parallel worker processes '
                             '(default: CPU count = %(default)s).')
    parser.add_argument('--strip-metadata', nargs='*', default=None,
                        metavar='PROFILE',
                        help=f'Remove unwanted metadata groups from output files. '
                             f'Available profiles: {_profiles}, all.  '
                             f'With no profile names, all profiles are enabled.')
    parser.add_argument('--tz-fallback', default=None, metavar='OFFSET',
                        help='Fallback timezone offset (e.g. +02:00, -05:30) '
                             'used when no EXIF timezone is found and no '
                             '--tz-override matches.  Defaults to the host '
                             "machine's local timezone.")
    parser.add_argument('--tz-override', action='append', default=[],
                        metavar='"START_UTC,END_UTC,OFFSET"',
                        help='Override the fallback timezone for files whose '
                             'UTC timestamp falls within the given range.  '
                             'Repeatable.  Format: '
                             '"YYYY-MM-DD HH:MM:SS,YYYY-MM-DD HH:MM:SS,+HH:MM".')
    parser.add_argument('--jpeg-quality-threshold', type=int, default=None,
                        metavar='PERCENT',
                        help='Recompress JPEG images whose estimated quality '
                             'exceeds this threshold (1-100).  JPEGs at or '
                             'below this quality are copied as-is.  Requires '
                             'Pillow.  Default: disabled (no recompression).  '
                             'Use 80 for a good balance of size and quality.')
    parser.add_argument('--jpeg-quality-skip-editor', action='append', default=[],
                        dest='jpeg_quality_skip_editors', metavar='NAME',
                        help='Skip JPEG compression for images exported from the '
                             'named editing software (case-insensitive substring '
                             'match against registry keys; repeatable).  '
                             'Use --list-editors to see available editors.  '
                             'Requires --jpeg-quality-threshold.')
    parser.add_argument('--jpeg-quality-skip-timerange', action='append', default=[],
                        dest='jpeg_quality_skip_timeranges',
                        metavar='"START,END,OFFSET"',
                        help='Skip JPEG compression for images whose resolved '
                             'datetime falls within the given time range.  '
                             'Repeatable.  Format: '
                             '"YYYY-MM-DD HH:MM:SS,YYYY-MM-DD HH:MM:SS,+HH:MM".  '
                             'Requires --jpeg-quality-threshold.')
    parser.add_argument('--list-editors', action='store_true',
                        help='Print available editor software names and exit.')

    # Handle --list-editors before parse_args (avoids requiring positional args)
    if '--list-editors' in sys.argv:
        print('Available editor software patterns:')
        for i, (key, pattern) in enumerate(EDITOR_SOFTWARE_PATTERNS.items(), 1):
            match_str = ', '.join(pattern['match'])
            excl = pattern.get('exclude')
            suffix = f'  (excludes: {", ".join(excl)})' if excl else ''
            print(f'  {i:>2}. {key:15s}  matches: {match_str}{suffix}')
        sys.exit(0)

    args = parser.parse_args()

    # Resolve strip profiles
    strip_profiles: Optional[List[str]] = None
    if args.strip_metadata is not None:
        strip_profiles = args.strip_metadata if args.strip_metadata else ['all']
    metadata_strip_params = _build_strip_params(strip_profiles)

    # Resolve timezone overrides
    tz_overrides: List[TimezoneOverride] = []
    for val in args.tz_override:
        try:
            tz_overrides.append(_parse_tz_override(val))
        except ValueError as e:
            parser.error(f"Invalid --tz-override: {e}")

    # Resolve fallback timezone
    fallback_tz: Optional[timezone] = None
    if args.tz_fallback is not None:
        fallback_tz = _parse_tz_offset(args.tz_fallback)
        if fallback_tz is None:
            parser.error(
                f"Invalid --tz-fallback value: {args.tz_fallback!r}\n"
                "Expected format: +HH:MM or -HH:MM (e.g. +02:00, -05:30)")

    # Validate JPEG quality
    jpeg_compress_quality: Optional[int] = args.jpeg_quality_threshold
    if jpeg_compress_quality is not None:
        if not 1 <= jpeg_compress_quality <= 100:
            parser.error(
                f"--jpeg-quality-threshold must be between 1 and 100, got {jpeg_compress_quality}")

    # Resolve editor skip patterns
    editor_skip_patterns: Optional[List[Dict[str, List[str]]]] = None
    if args.jpeg_quality_skip_editors:
        if jpeg_compress_quality is None:
            logging.warning("--jpeg-quality-skip-editor has no effect without --jpeg-quality-threshold")
        try:
            editor_skip_patterns = _resolve_editor_skip_patterns(
                args.jpeg_quality_skip_editors)
        except ValueError as e:
            parser.error(str(e))

    # Resolve JPEG compression skip timeranges
    jpeg_skip_timeranges: List[JpegSkipTimerange] = []
    for val in args.jpeg_quality_skip_timeranges:
        try:
            jpeg_skip_timeranges.append(_parse_jpeg_skip_timerange(val))
        except ValueError as e:
            parser.error(f"Invalid --jpeg-quality-skip-timerange: {e}")
    if jpeg_skip_timeranges and jpeg_compress_quality is None:
        logging.warning("--jpeg-quality-skip-timerange has no effect without --jpeg-quality-threshold")

    blocked_descriptions = [
        # Add unwanted description strings here
        "SONY DSC",
        "OLYMPUS DIGITAL CAMERA",
    ]

    merger = PhotosExportMerger(args.input_dir, args.output_dir,
                                dry_run=args.dry_run,
                                blocked_descriptions=blocked_descriptions,
                                num_workers=args.workers,
                                metadata_strip_params=metadata_strip_params,
                                tz_overrides=tz_overrides or None,
                                fallback_tz=fallback_tz,
                                jpeg_compress_quality=jpeg_compress_quality,
                                editor_skip_patterns=editor_skip_patterns,
                                jpeg_compress_skip_timeranges=jpeg_skip_timeranges or None)
    result = merger.run()