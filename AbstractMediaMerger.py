from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import enum
import logging


class WriteStrategy(enum.Enum):
    DIRECT = 1                  # Write all tags to file, no sidecar
    PARTIAL_WITH_SIDECAR = 2    # Write what's possible + XMP sidecar
    VIDEO_WITH_SIDECAR = 3      # Always XMP sidecar + write to file where possible


@dataclass
class MediaFileInfo:
    source_path: Path
    filename: str
    json_data: Optional[Dict[str, Any]] = None
    new_title: Optional[str] = None
    output_path: Optional[Path] = None
    sidecar_path: Optional[Path] = None
    write_strategy: Optional[WriteStrategy] = None
    year: Optional[str] = None
    month: Optional[str] = None
    is_orphan: bool = False
    clear_descriptions: bool = False
    has_iptc_caption: bool = False
    date_source: Optional[str] = None
    resolved_datetime: Optional[datetime] = None
    error: Optional[str] = None
    # Pre-extracted fields for processing (avoids shipping full json_data to workers)
    description: Optional[str] = None
    gps: Optional[Dict[str, float]] = None
    # XMP date tags present in source file (conditionally updated during processing)
    existing_xmp_dates: Optional[Set[str]] = None
    # Actual file extension (from ExifTool FileTypeExtension) when it differs
    # from the source file's extension (e.g. JPEG content with .DNG extension).
    # None when the extension matches or is unknown.
    actual_ext: Optional[str] = None


@dataclass
class MergeStats:
    total_media_files: int = 0
    matched: int = 0
    orphans: int = 0
    written: int = 0
    sidecars_created: int = 0
    errors: int = 0
    skipped_json: int = 0
    duplicates_renamed: int = 0
    date_from_exif: int = 0
    date_from_filesystem: int = 0
    gps_written: int = 0
    descriptions_cleared: int = 0
    ext_mismatches: int = 0
    skipped_existing: int = 0

    def merge(self, other: 'MergeStats') -> None:
        """Add all counters from *other* into this instance.

        Used to aggregate partial stats returned by parallel workers.
        Only processing counters are merged; pipeline-level counters
        (total_media_files, matched, orphans, skipped_json, duplicates_renamed,
        ext_mismatches) are set before parallelisation and should not be
        summed again.
        """
        self.written += other.written
        self.sidecars_created += other.sidecars_created
        self.errors += other.errors
        self.gps_written += other.gps_written
        self.descriptions_cleared += other.descriptions_cleared
        self.skipped_existing += other.skipped_existing


def _resolve_gps(json_data: Dict[str, Any]) -> Optional[Dict[str, float]]:
    """Extract valid GPS data from JSON. Returns dict with lat, lon, alt or None."""
    for key in ('geoData', 'geoDataExif'):
        geo = json_data.get(key)
        if geo:
            lat = geo.get('latitude', 0.0)
            lon = geo.get('longitude', 0.0)
            if lat != 0.0 or lon != 0.0:
                return {
                    'latitude': lat,
                    'longitude': lon,
                    'altitude': geo.get('altitude', 0.0),
                }
    return None


class AbstractMediaMerger(ABC):
    def __init__(self, input_dir: str, output_dir: str, dry_run: bool = False,
                 blocked_descriptions=None, num_workers: int = 1):
        self.input_path = Path(input_dir).resolve()
        self.output_path = Path(output_dir).resolve()
        self.dry_run = dry_run
        self.num_workers = max(1, num_workers)
        self.blocked_descriptions: set = set(blocked_descriptions) if blocked_descriptions else set()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S'))
            self.logger.addHandler(handler)
        self.logger.propagate = False

    def run(self) -> MergeStats:
        stats = MergeStats()

        # Step 1: Validate directories
        self._validate_directories()

        self._open_writer()
        try:
            # Step 2: Scan files
            media_by_dir, metadata_by_dir = self._scan_files()

            # Step 3: Match metadata to media
            media_files, referenced_by_dir = self._match_metadata_to_media(
                media_by_dir, metadata_by_dir, stats
            )

            # Step 4: Identify orphans
            orphan_files = self._identify_orphans(media_by_dir, referenced_by_dir)
            stats.orphans = len(orphan_files)
            media_files.extend(orphan_files)
            stats.total_media_files = len(media_files)

            # Step 5: Resolve dates and output paths
            self._resolve_dates_and_paths(media_files, stats)

            # Step 6: Resolve duplicate filenames
            self._resolve_duplicates(media_files, stats)
        finally:
            self._close_writer()

        # Step 7 & 8: Process files
        # Writer lifecycle is managed by _process_files: serial mode opens/closes
        # the writer; parallel mode lets each worker open its own.
        self._process_files(media_files, stats)

        # Step 9: Log summary
        self._log_summary(stats)
        return stats

    def _open_writer(self) -> None:
        """Lifecycle hook: called before processing begins. Override to open external tools."""
        pass

    def _close_writer(self) -> None:
        """Lifecycle hook: called after processing ends (always, in a finally block). Override to clean up."""
        pass

    @abstractmethod
    def _validate_directories(self) -> None:
        """Raise on bad paths; create output_path unless dry_run."""
        ...

    @abstractmethod
    def _scan_files(self) -> Tuple[Dict[Path, Any], Dict[Path, Any]]:
        """Scan the input directory tree. Returns (media_by_dir, metadata_by_dir)."""
        ...

    @abstractmethod
    def _match_metadata_to_media(
        self,
        media_by_dir: Dict[Path, Any],
        metadata_by_dir: Dict[Path, Any],
        stats: MergeStats,
    ) -> Tuple[List[MediaFileInfo], Dict[Path, Set]]:
        """Match metadata files to their corresponding media files.
        Increments stats.matched and stats.skipped_json.
        Returns (media_files, referenced_by_dir)."""
        ...

    @abstractmethod
    def _identify_orphans(
        self,
        media_by_dir: Dict[Path, Any],
        referenced_by_dir: Dict[Path, Set],
    ) -> List[MediaFileInfo]:
        """Return MediaFileInfo for all media files without matching metadata.
        Does NOT modify stats."""
        ...

    @abstractmethod
    def _resolve_dates_and_paths(
        self,
        media_files: List[MediaFileInfo],
        stats: MergeStats,
    ) -> None:
        """Mutate each MediaFileInfo in-place: set output_path, sidecar_path,
        resolved_datetime, year, month."""
        ...

    @abstractmethod
    def _process_matched(self, info: MediaFileInfo, stats: MergeStats) -> None:
        """Write a matched media file and its metadata to the output directory."""
        ...

    @abstractmethod
    def _process_orphan(self, info: MediaFileInfo, stats: MergeStats) -> None:
        """Copy an orphan media file (no metadata) to the output directory."""
        ...

    def _resolve_duplicates(self, media_files: List[MediaFileInfo], stats: MergeStats) -> None:
        seen: Dict[Path, int] = {}
        for info in media_files:
            if info.output_path is None:
                continue
            path = info.output_path
            if path in seen:
                seen[path] += 1
                counter = seen[path]
                stem = path.stem
                ext = path.suffix
                new_name = f"{stem}_{counter}{ext}"
                original_path = info.output_path
                info.output_path = path.parent / new_name
                info.new_title = new_name
                if info.sidecar_path:
                    info.sidecar_path = path.parent / f"{stem}_{counter}{ext}.xmp"
                stats.duplicates_renamed += 1
                self.logger.warning(
                    "Duplicate filename resolved: %s -> %s (source: %s)",
                    original_path.name, new_name, self._rel(info.source_path),
                )
            else:
                seen[path] = 1

    def _process_files(self, media_files: List[MediaFileInfo], stats: MergeStats) -> None:
        # Filter out files with no output path before dispatching
        valid_files: List[MediaFileInfo] = []
        for info in media_files:
            if info.output_path is None:
                self.logger.error("No output path for %s, skipping", self._rel(info.source_path))
                stats.errors += 1
            else:
                valid_files.append(info)

        if not valid_files:
            return

        if self.num_workers > 1 and not self.dry_run and len(valid_files) > 1:
            # Parallel mode: each worker opens its own writer
            self._process_files_parallel(valid_files, stats)
        else:
            # Serial mode: open a single writer for all files
            self._open_writer()
            try:
                self._process_files_serial(valid_files, stats)
            finally:
                self._close_writer()

    def _process_files_serial(self, media_files: List[MediaFileInfo], stats: MergeStats) -> None:
        """Process files one at a time using the current ExifTool instance."""
        for info in media_files:
            try:
                if info.is_orphan:
                    self._process_orphan(info, stats)
                else:
                    self._process_matched(info, stats)
            except Exception as e:
                self.logger.error("Failed to process %s: %s", self._rel(info.source_path), e)
                stats.errors += 1

    def _process_files_parallel(self, media_files: List[MediaFileInfo], stats: MergeStats) -> None:
        """Override in subclass to implement parallel processing.

        Falls back to serial if not overridden.
        """
        self._process_files_serial(media_files, stats)

    def _log_dry_run(self, info: MediaFileInfo) -> None:
        kind = "ORPHAN" if info.is_orphan else "MATCHED"
        strategy_name = info.write_strategy.name if info.write_strategy else "UNKNOWN"
        sidecar = f" + sidecar: {info.sidecar_path.name}" if info.sidecar_path else ""

        self.logger.info("[DRY RUN] [%s] %s", kind, self._rel(info.source_path))
        self.logger.info("  Source: %s", info.source_path)
        self.logger.info("  Dest:   %s", info.output_path)
        self.logger.info("  Strategy: %s%s", strategy_name, sidecar)

        if info.resolved_datetime:
            self.logger.info("  Date: %s (source: %s)", info.resolved_datetime.isoformat(), info.date_source)

        if info.clear_descriptions:
            if info.has_iptc_caption:
                self.logger.info("  Blocked description detected — will clear UserComment, ImageDescription, XMP:Description, IPTC:Caption-Abstract")
            else:
                self.logger.info("  Blocked description detected — will clear UserComment, ImageDescription, XMP:Description")

        if not info.is_orphan:
            tags = []
            if info.resolved_datetime:
                tags.append('dates')
            if info.description:
                tags.append(f'description="{info.description}"')
            if info.gps:
                tags.append(f'GPS({info.gps["latitude"]:.4f}, {info.gps["longitude"]:.4f})')
            if tags:
                self.logger.info("  Tags to write: %s", ', '.join(tags))

    def _log_summary(self, stats: MergeStats) -> None:
        self.logger.info("=" * 60)
        self.logger.info("MERGE SUMMARY%s", " (DRY RUN)" if self.dry_run else "")
        self.logger.info("=" * 60)
        self.logger.info("Total media files:            %d", stats.total_media_files)
        self.logger.info("Matched (with JSON):          %d", stats.matched)
        self.logger.info("Orphans (no JSON):            %d", stats.orphans)
        self.logger.info("Files written:                %d", stats.written)
        self.logger.info("XMP sidecars created:         %d", stats.sidecars_created)
        self.logger.info("GPS tags written:             %d", stats.gps_written)
        self.logger.info("Descriptions cleared:         %d", stats.descriptions_cleared)
        self.logger.info("Duplicates renamed:           %d", stats.duplicates_renamed)
        self.logger.info("Skipped JSON files:           %d", stats.skipped_json)
        self.logger.info("Ext mismatches fixed:         %d", stats.ext_mismatches)
        self.logger.info("Skipped (existing):           %d", stats.skipped_existing)
        self.logger.info("Errors:                       %d", stats.errors)
        if stats.date_from_exif > 0:
            self.logger.info("Orphan dates from EXIF:       %d", stats.date_from_exif)
        if stats.date_from_filesystem > 0:
            self.logger.info("Orphan dates from filesystem: %d", stats.date_from_filesystem)
        self.logger.info("=" * 60)

    def _rel(self, path: Path) -> str:
        """Return path relative to input directory for log readability."""
        try:
            return str(path.relative_to(self.input_path))
        except ValueError:
            return str(path)