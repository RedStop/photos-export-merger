from AbstractMediaMerger import (AbstractMediaMerger, WriteStrategy,
                                  MediaFileInfo, MergeStats, _resolve_gps)
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from JsonFileIdentifier import JsonFileFinder
from pathlib import Path
from sortedcontainers import SortedSet
from typing import Dict, List, Optional, Any
import exiftool
import json
import logging
import shutil
import sys


DIRECT_WRITE_EXTS = {'.jpg', '.jpeg', '.tiff', '.tif', '.dng', '.cr2', '.heic'}
PARTIAL_WRITE_EXTS = {'.png', '.gif'}
VIDEO_EXTS = {'.avi', '.mkv', '.mov', '.mp4', '.m4v', '.webm'}
ALL_MEDIA_EXTS = DIRECT_WRITE_EXTS | PARTIAL_WRITE_EXTS | VIDEO_EXTS

DATE_TAGS_PRIORITY = [
    'EXIF:DateTimeOriginal',
    'EXIF:CreateDate',
    'QuickTime:CreateDate',
    'QuickTime:MediaCreateDate',
    'EXIF:ModifyDate',
]

DESC_READ_TAGS = ['EXIF:UserComment', 'EXIF:ImageDescription', 'XMP:Description']

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


def _build_sidecar_params(info: MediaFileInfo, gps: Optional[Dict[str, float]]) -> List[str]:
    """Build ExifTool params for creating an XMP sidecar."""
    params = ['-charset', 'filename=utf8']

    if info.resolved_datetime:
        tz_str = _format_tz_offset(info.resolved_datetime.tzinfo)
        dt_str = info.resolved_datetime.strftime('%Y:%m:%d %H:%M:%S') + tz_str
        params.append(f'-XMP:DateTimeOriginal={dt_str}')
        params.append(f'-XMP:CreateDate={dt_str}')
        params.append(f'-XMP:ModifyDate={dt_str}')

    if not info.clear_descriptions and info.json_data:
        desc = info.json_data.get('description', '')
        if desc:
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
    Returns (escaped_desc, needs_E_flag)."""
    if '\n' not in desc and '\r' not in desc:
        return desc, False
    escaped = desc.replace('&', '&amp;')
    escaped = escaped.replace('\n', '&#xa;')
    escaped = escaped.replace('\r', '&#xd;')
    return escaped, True


class GooglePhotosExportMerger(AbstractMediaMerger):
    def __init__(self, input_dir: str, output_dir: str, dry_run: bool = False,
                 blocked_descriptions: Optional[List[str]] = None):
        super().__init__(input_dir, output_dir, dry_run, blocked_descriptions)

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
            read_tags = tz_tags + (DESC_READ_TAGS if self.blocked_descriptions else [])

            try:
                tag_results = self._et.get_tags(file_paths, read_tags)
            except Exception as e:
                self.logger.warning("Failed to batch-read timezone from %s: %s", self._rel(dir_path), e)
                tag_results = [{} for _ in infos]

            for info, tags in zip(infos, tag_results):
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
            read_tags = DATE_TAGS_PRIORITY + (DESC_READ_TAGS if self.blocked_descriptions else [])

            try:
                tag_results = self._et.get_tags(file_paths, read_tags)
            except Exception as e:
                self.logger.warning("Failed to batch-read dates from %s: %s", self._rel(dir_path), e)
                tag_results = [{} for _ in infos]

            for info, tags in zip(infos, tag_results):
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

    def _process_matched(self, info: MediaFileInfo, stats: MergeStats):
        if self.dry_run:
            self._log_dry_run(info)
            return

        # Ensure output directory exists
        info.output_path.parent.mkdir(parents=True, exist_ok=True)

        # Build ExifTool params (NO -overwrite_original: -o copies to output without deleting source)
        params = ['-charset', 'filename=utf8']

        # Date tags
        if info.resolved_datetime:
            dt_str = info.resolved_datetime.strftime('%Y:%m:%d %H:%M:%S')
            tz_str = _format_tz_offset(info.resolved_datetime.tzinfo)
            params.append(f'-alldates={dt_str}')
            params.append(f'-EXIF:ExifIFD:OffsetTime={tz_str}')
            params.append(f'-EXIF:ExifIFD:OffsetTimeOriginal={tz_str}')
            params.append(f'-EXIF:ExifIFD:OffsetTimeDigitized={tz_str}')

        # Description
        if info.clear_descriptions:
            params.append('-EXIF:UserComment=')
            params.append('-EXIF:ImageDescription=')
            params.append('-XMP-dc:Description=')
            stats.descriptions_cleared += 1
        elif info.json_data:
            desc = info.json_data.get('description', '')
            if desc:
                escaped, needs_E = _escape_description(desc)
                if needs_E:
                    params.append('-E')
                params.append(f'-XMP-dc:Description={escaped}')
                params.append(f'-EXIF:ImageDescription={escaped}')

        # GPS
        gps = None
        if info.json_data:
            gps = _resolve_gps(info.json_data)
            if gps:
                params.extend(_build_gps_params(gps))
                stats.gps_written += 1

        # Use -o to copy source to output with metadata in one step
        params.append('-o')
        params.append(str(info.output_path))
        params.append(str(info.source_path))

        try:
            self._et.execute(*[p.encode('utf-8') if isinstance(p, str) else p for p in params])
        except Exception as e:
            if info.output_path.exists():
                # ExifTool status 1 = warnings (e.g. unsupported tags for this format).
                # The -o copy still succeeded, so treat as success with warnings.
                self.logger.debug("ExifTool warnings for %s: %s (output created successfully)",
                                  self._rel(info.source_path), e)
            else:
                # Genuine failure: output not created. Fallback to copy + in-place write.
                self.logger.warning("ExifTool -o failed for %s: %s, falling back to copy+write",
                                    self._rel(info.source_path), e)
                try:
                    shutil.copy2(str(info.source_path), str(info.output_path))
                    fallback_params = ['-charset', 'filename=utf8', '-overwrite_original']
                    fallback_params.extend(params[2:-3])  # tag params only (skip charset/filename, -o/output/source)
                    fallback_params.append(str(info.output_path))
                    self._et.execute(*[p.encode('utf-8') if isinstance(p, str) else p for p in fallback_params])
                except Exception as e2:
                    if not info.output_path.exists():
                        self.logger.error("Failed to process %s: %s", self._rel(info.source_path), e2)
                        stats.errors += 1
                        return

        stats.written += 1

        # Create XMP sidecar if needed
        if info.sidecar_path:
            self._create_sidecar(info, gps, stats)

        # Set filesystem timestamps
        self._set_filesystem_timestamps(info)

    def _process_orphan(self, info: MediaFileInfo, stats: MergeStats):
        if self.dry_run:
            self._log_dry_run(info)
            return

        info.output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            shutil.copy2(str(info.source_path), str(info.output_path))
            stats.written += 1
        except Exception as e:
            self.logger.error("Failed to copy orphan %s: %s", self._rel(info.source_path), e)
            stats.errors += 1
            return

        # Clear blocked descriptions from output file
        if info.clear_descriptions:
            try:
                clear_params = [
                    '-overwrite_original',
                    '-EXIF:UserComment=',
                    '-EXIF:ImageDescription=',
                    '-XMP-dc:Description=',
                    str(info.output_path),
                ]
                self._et.execute(*[p.encode('utf-8') if isinstance(p, str) else p for p in clear_params])
                stats.descriptions_cleared += 1
            except Exception as e:
                self.logger.warning("Failed to clear descriptions for orphan %s: %s", self._rel(info.source_path), e)

        # Set filesystem timestamps if date was resolved
        self._set_filesystem_timestamps(info)

    def _create_sidecar(self, info: MediaFileInfo, gps: Optional[Dict[str, float]], stats: MergeStats):
        if info.sidecar_path is None:
            return

        sidecar_params = _build_sidecar_params(info, gps)

        # Create the XMP from scratch by writing tags to a new file
        sidecar_params.append('-o')
        sidecar_params.append(str(info.sidecar_path))
        sidecar_params.append(str(info.output_path))

        try:
            self._et.execute(*[p.encode('utf-8') if isinstance(p, str) else p for p in sidecar_params])
            stats.sidecars_created += 1
            self.logger.info("Created XMP sidecar for %s: %s", self._rel(info.source_path), info.sidecar_path.name)
        except Exception as e:
            self.logger.warning("Failed to create XMP sidecar for %s: %s", self._rel(info.source_path), e)

    def _set_filesystem_timestamps(self, info: MediaFileInfo):
        if info.resolved_datetime is None:
            return

        # Always convert to GMT+02:00 for filesystem timestamps
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
                self._et.execute(*[p.encode('utf-8') if isinstance(p, str) else p for p in ts_params])
            except Exception as e:
                self.logger.warning("Failed to set filesystem timestamps for %s: %s", self._rel(info.source_path), e)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print(f"Usage: python {sys.argv[0]} <input_dir> <output_dir> [--dry-run]")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    dry_run = '--dry-run' in sys.argv

    blocked_descriptions = [
        # Add unwanted description strings here, e.g.:
        # "Photo uploaded by Google Photos",
        "          ",
        "SONY DSC",
        "OLYMPUS DIGITAL CAMERA",
        "DCIM\\100MEDIA\\DJI_0009.JPG",
        "DCIM\\100MEDIA\\DJI_0021.JPG",
        "DCIM\\100MEDIA\\DJI_0036.JPG",
        "DCIM\\100MEDIA\\DJI_0040.JPG",
    ]

    merger = GooglePhotosExportMerger(input_dir, output_dir, dry_run=dry_run,
                                     blocked_descriptions=blocked_descriptions)
    result = merger.run()
