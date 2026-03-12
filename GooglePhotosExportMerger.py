from AbstractMediaMerger import (AbstractMediaMerger, WriteStrategy,
                                  MediaFileInfo, MergeStats, _resolve_gps)
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from JsonFileIdentifier import JsonFileFinder
from pathlib import Path
from sortedcontainers import SortedSet
from typing import Dict, List, Optional, Any
import exiftool
import json
import logging
import os
import shutil
import sys


DIRECT_WRITE_EXTS = {'.jpg', '.jpeg', '.tiff', '.tif', '.dng', '.cr2', '.heic'}
PARTIAL_WRITE_EXTS = {'.png', '.gif'}
VIDEO_EXTS = {'.avi', '.mkv', '.mov', '.mp4', '.m4v', '.webm'}
# QuickTime-based containers where ExifTool can write QT/UserData/XMP tags
# directly into the file.  Non-QT video containers (AVI, MKV, WebM) are
# copy-only; all metadata lives in the XMP sidecar.
QUICKTIME_VIDEO_EXTS = {'.mov', '.mp4', '.m4v'}
ALL_MEDIA_EXTS = DIRECT_WRITE_EXTS | PARTIAL_WRITE_EXTS | VIDEO_EXTS

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
    'IPTC:DateCreated':         lambda dt, tz: [f'-IPTC:DateCreated={dt[:10]}'],
    'IPTC:TimeCreated':         lambda dt, tz: [f'-IPTC:TimeCreated={dt[11:]}{tz}'],
    'IPTC:DigitalCreationDate': lambda dt, tz: [f'-IPTC:DigitalCreationDate={dt[:10]}'],
    'IPTC:DigitalCreationTime': lambda dt, tz: [f'-IPTC:DigitalCreationTime={dt[11:]}{tz}'],
}

# Read-tag list for batch reads (keys of the mapping above).
CONDITIONAL_DATE_READ_TAGS: List[str] = list(CONDITIONAL_DATE_TAGS.keys())

GMT_PLUS_2 = timezone(timedelta(hours=2))


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

    Only applies to DIRECT and PARTIAL_WITH_SIDECAR strategies.
    Uses CONDITIONAL_DATE_TAGS mapping to translate read-tag names into the
    correct write parameters.
    """
    if not info.existing_xmp_dates:
        return []
    if info.write_strategy == WriteStrategy.VIDEO_WITH_SIDECAR:
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


def _do_process_matched(et, info: MediaFileInfo, stats: MergeStats,
                        logger: logging.Logger):
    """Core logic for processing a matched media file."""
    info.output_path.parent.mkdir(parents=True, exist_ok=True)

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
        # Stats for metadata that will be written to the XMP sidecar.
        if info.gps:
            stats.gps_written += 1
        if info.clear_descriptions:
            stats.descriptions_cleared += 1
    else:
        params = ['-charset', 'filename=utf8']

        if info.resolved_datetime:
            dt_str = info.resolved_datetime.strftime('%Y:%m:%d %H:%M:%S')
            tz_str = _format_tz_offset(info.resolved_datetime.tzinfo)

            if is_qt_video:
                # QuickTime spec stores dates as UTC (no timezone field).
                # Convert local resolved_datetime back to UTC for QT tags.
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
                # PNG/GIF lack EXIF; -alldates writes to XMP but drops the
                # timezone suffix.  Write XMP date tags explicitly so the
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

            # Update any pre-existing XMP date tags to the resolved datetime
            # (e.g. XMP-photoshop:DateCreated, XMP-xmp:MetadataDate).
            params.extend(_build_conditional_date_params(info, dt_str, tz_str))

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
        _do_create_sidecar(et, info, stats, logger)

    _do_set_filesystem_timestamps(et, info, logger)


def _do_process_orphan(et, info: MediaFileInfo, stats: MergeStats,
                       logger: logging.Logger):
    """Core logic for processing an orphan media file."""
    info.output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        shutil.copy2(str(info.source_path), str(info.output_path))
        stats.written += 1
    except Exception as e:
        logger.error("Failed to copy orphan %s: %s", info.source_path, e)
        stats.errors += 1
        return

    # Build params for in-place updates (descriptions + dates).
    update_params = ['-charset', 'filename=utf8', '-overwrite_original']

    if info.clear_descriptions:
        update_params.append('-EXIF:UserComment=')
        update_params.append('-EXIF:ImageDescription=')
        update_params.append('-XMP-dc:Description=')
        if info.has_iptc_caption:
            update_params.append('-IPTC:Caption-Abstract=')
        stats.descriptions_cleared += 1

    # Fill in any missing date tags and add timezone offsets.
    # Non-QuickTime video containers cannot have tags written in-place.
    is_non_qt_video = (info.write_strategy == WriteStrategy.VIDEO_WITH_SIDECAR
                       and info.source_path.suffix.lower() not in QUICKTIME_VIDEO_EXTS)

    if info.resolved_datetime and not is_non_qt_video:
        dt_str = info.resolved_datetime.strftime('%Y:%m:%d %H:%M:%S')
        tz_str = _format_tz_offset(info.resolved_datetime.tzinfo)

        if info.write_strategy == WriteStrategy.VIDEO_WITH_SIDECAR:
            utc_dt = info.resolved_datetime.astimezone(timezone.utc)
            utc_str = utc_dt.strftime('%Y:%m:%d %H:%M:%S')
            update_params.append(f'-QuickTime:CreateDate={utc_str}')
            update_params.append(f'-QuickTime:ModifyDate={utc_str}')
            local_with_tz = f'{dt_str}{tz_str}'
            update_params.append(f'-UserData:DateTimeOriginal={local_with_tz}')
            update_params.append(f'-XMP-exif:DateTimeOriginal={local_with_tz}')
            update_params.append(f'-XMP-xmp:CreateDate={local_with_tz}')
            update_params.append(f'-XMP-xmp:ModifyDate={local_with_tz}')
        elif info.write_strategy == WriteStrategy.PARTIAL_WITH_SIDECAR:
            local_with_tz = f'{dt_str}{tz_str}'
            update_params.append(f'-XMP-exif:DateTimeOriginal={local_with_tz}')
            update_params.append(f'-XMP-xmp:CreateDate={local_with_tz}')
            update_params.append(f'-XMP-xmp:ModifyDate={local_with_tz}')
        else:
            update_params.append(f'-alldates={dt_str}')
            update_params.append(f'-EXIF:ExifIFD:OffsetTime={tz_str}')
            update_params.append(f'-EXIF:ExifIFD:OffsetTimeOriginal={tz_str}')
            update_params.append(f'-EXIF:ExifIFD:OffsetTimeDigitized={tz_str}')

        # Update any pre-existing XMP date tags to the resolved datetime.
        update_params.extend(_build_conditional_date_params(info, dt_str, tz_str))

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


def _do_set_filesystem_timestamps(et, info: MediaFileInfo, logger: logging.Logger):
    """Core logic for setting filesystem timestamps."""
    if info.resolved_datetime is None:
        return

    gmt2_dt = info.resolved_datetime.astimezone(GMT_PLUS_2)
    dt_str = gmt2_dt.strftime('%Y:%m:%d %H:%M:%S') + '+02:00'

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


# ---------------------------------------------------------------------------
# Parallel worker — top-level function for ProcessPoolExecutor pickling
# ---------------------------------------------------------------------------

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


class GooglePhotosExportMerger(AbstractMediaMerger):
    def __init__(self, input_dir: str, output_dir: str, dry_run: bool = False,
                 blocked_descriptions: Optional[List[str]] = None,
                 num_workers: int = 1):
        super().__init__(input_dir, output_dir, dry_run, blocked_descriptions,
                         num_workers=num_workers)

    def _open_writer(self) -> None:
        self._et_helper = exiftool.ExifToolHelper()
        self._et = self._et_helper.__enter__()

    def _close_writer(self) -> None:
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

        # Resolve timezone for matched files (batch read per directory)
        for dir_path, infos in matched_by_dir.items():
            file_paths = [str(info.source_path) for info in infos]
            tz_tags = ['EXIF:OffsetTimeOriginal', 'EXIF:OffsetTime']
            read_tags = tz_tags + CONDITIONAL_DATE_READ_TAGS + ['File:FileTypeExtension'] + (DESC_READ_TAGS if self.blocked_descriptions else ['IPTC:Caption-Abstract'])

            try:
                tag_results = self._et.get_tags(file_paths, read_tags)
            except Exception as e:
                self.logger.warning("Failed to batch-read timezone from %s: %s", self._rel(dir_path), e)
                tag_results = [{} for _ in infos]

            for info, tags in zip(infos, tag_results):
                # Track whether source file has IPTC:Caption-Abstract
                if tags.get('IPTC:Caption-Abstract'):
                    info.has_iptc_caption = True

                # Record which XMP date tags exist in the source file
                # (used to conditionally update them during processing).
                existing = {t for t in CONDITIONAL_DATE_READ_TAGS if tags.get(t)}
                if existing:
                    info.existing_xmp_dates = existing

                # Detect extension/content mismatch (e.g. JPEG with .DNG extension).
                actual_type_ext = tags.get('File:FileTypeExtension')
                if actual_type_ext:
                    actual = f'.{actual_type_ext.lower()}'
                    source = info.source_path.suffix.lower()
                    if actual != source:
                        info.actual_ext = actual
                        self.logger.info("Extension mismatch for %s: content is %s",
                                         self._rel(info.source_path), actual_type_ext)

                # Check blocked descriptions
                if self.blocked_descriptions and info.json_data:
                    json_desc = info.json_data.get('description', '')
                    if json_desc and json_desc in self.blocked_descriptions:
                        info.clear_descriptions = True
                    elif not json_desc:
                        for desc_tag in DESC_READ_TAGS:
                            exif_desc = tags.get(desc_tag, '')
                            if exif_desc and str(exif_desc) in self.blocked_descriptions:
                                info.clear_descriptions = True
                                break

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
                    tz = GMT_PLUS_2
                    self.logger.warning("No timezone in EXIF for %s, using GMT+02:00 fallback", self._rel(info.source_path))

                utc_dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
                local_dt = utc_dt.astimezone(tz)
                info.resolved_datetime = local_dt
                info.date_source = 'json_photoTakenTime'
                info.year = local_dt.strftime('%Y')
                info.month = local_dt.strftime('%m')

        # Resolve dates for orphan files (batch read per directory)
        for dir_path, infos in orphans_by_dir.items():
            file_paths = [str(info.source_path) for info in infos]
            read_tags = DATE_TAGS_PRIORITY + CONDITIONAL_DATE_READ_TAGS + ['File:FileTypeExtension'] + (DESC_READ_TAGS if self.blocked_descriptions else ['IPTC:Caption-Abstract'])

            try:
                tag_results = self._et.get_tags(file_paths, read_tags)
            except Exception as e:
                self.logger.warning("Failed to batch-read dates from %s: %s", self._rel(dir_path), e)
                tag_results = [{} for _ in infos]

            for info, tags in zip(infos, tag_results):
                # Track whether source file has IPTC:Caption-Abstract
                if tags.get('IPTC:Caption-Abstract'):
                    info.has_iptc_caption = True

                # Record which XMP date tags exist in the source file.
                existing = {t for t in CONDITIONAL_DATE_READ_TAGS if tags.get(t)}
                if existing:
                    info.existing_xmp_dates = existing

                # Detect extension/content mismatch.
                actual_type_ext = tags.get('File:FileTypeExtension')
                if actual_type_ext:
                    actual = f'.{actual_type_ext.lower()}'
                    source = info.source_path.suffix.lower()
                    if actual != source:
                        info.actual_ext = actual
                        self.logger.info("Extension mismatch for orphan %s: content is %s",
                                         self._rel(info.source_path), actual_type_ext)

                # Check blocked descriptions on existing EXIF tags
                if self.blocked_descriptions:
                    for desc_tag in DESC_READ_TAGS:
                        exif_desc = tags.get(desc_tag, '')
                        if exif_desc and str(exif_desc) in self.blocked_descriptions:
                            info.clear_descriptions = True
                            break

                resolved_dt = None
                date_source = None

                for tag_key in DATE_TAGS_PRIORITY:
                    val = tags.get(tag_key)
                    if val and isinstance(val, str) and val.strip():
                        try:
                            # EXIF dates are typically "YYYY:MM:DD HH:MM:SS"
                            parsed = datetime.strptime(val.strip()[:19], '%Y:%m:%d %H:%M:%S')
                            resolved_dt = parsed.replace(tzinfo=GMT_PLUS_2)
                            date_source = tag_key
                            stats.date_from_exif += 1
                            break
                        except ValueError:
                            continue

                if resolved_dt is None:
                    # Last resort: file creation date
                    try:
                        ctime = info.source_path.stat().st_ctime
                        resolved_dt = datetime.fromtimestamp(ctime, tz=GMT_PLUS_2)
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
    if len(sys.argv) < 3:
        print(f"Usage: python {sys.argv[0]} <input_dir> <output_dir> [--dry-run] [--workers N]")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    dry_run = '--dry-run' in sys.argv

    # Parse --workers N (default: CPU count)
    num_workers = os.cpu_count() or 1
    for i, arg in enumerate(sys.argv):
        if arg == '--workers' and i + 1 < len(sys.argv):
            try:
                num_workers = int(sys.argv[i + 1])
            except ValueError:
                print(f"Invalid --workers value: {sys.argv[i + 1]}")
                sys.exit(1)

    blocked_descriptions = [
        # Add unwanted description strings here, e.g.:
        # "Photo uploaded by Google Photos",
        "SONY DSC",
        "OLYMPUS DIGITAL CAMERA",
        "DCIM\\100MEDIA\\DJI_0009.JPG",
        "DCIM\\100MEDIA\\DJI_0021.JPG",
        "DCIM\\100MEDIA\\DJI_0036.JPG",
        "DCIM\\100MEDIA\\DJI_0040.JPG",
    ]

    merger = GooglePhotosExportMerger(input_dir, output_dir, dry_run=dry_run,
                                     blocked_descriptions=blocked_descriptions,
                                     num_workers=num_workers)
    result = merger.run()