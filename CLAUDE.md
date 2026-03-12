# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Google Photos Export Merger — a Python utility that merges JSON metadata from Google Photos Takeout exports into image/video EXIF properties using ExifTool. Developed and tested on Windows; setup scripts also support Linux/macOS/WSL, though the `FileCreateDate` tag used for filesystem creation times is Windows-only (on other platforms only the modification time is reliably set).

## Requirements

- Python 3.10.11 (virtual environment in `.venv/`)
- ExifTool 12.45 (must be in system PATH)
- PyExifTool 0.5.6, sortedcontainers 2.4.0

## Running

```bash
# Analyse JSON metadata structure across a directory tree
python JsonKeyExtractor.py <input_directory> [output_directory]

# Merge metadata into media files (uses all CPU cores by default)
python GooglePhotosExportMerger.py <input_dir> <output_dir> [--dry-run] [--workers N]

# Run tests
python -m pytest TestMerger.py
```

## Testing

`TestMerger.py` is a comprehensive `unittest`-based test suite with 185+ test methods (including subtests). It runs as a single-pass integration test: `setUpClass` builds an input tree with programmatically generated binary test files (JPEG, PNG, GIF, TIFF, CR2, DNG, HEIC, MP4, MOV, AVI, MKV, WebM — including variants with embedded EXIF timezone offsets and Nikon maker-note dates), runs the merger once, then individual tests assert on the output. Test categories include: input integrity, output structure, GPS (8 compass directions × 12 formats), timezones (including sidecar timezone verification), descriptions (UTF-8, escaping, newlines, blocked, IPTC), file types, orphan files, XMP conditional dates, XMP sidecars, duplicates, bracket notation, file timestamps, stats verification, video UTC time, special filenames, EXIF preservation, extension mismatch, video XMP dates (including Nikon maker-note sidecar fixup), infrastructure validation, and single-worker (serial) mode.

A `TestSingleWorker` class re-runs the merger with `num_workers=1` and verifies stats and output match the parallel run.

## Architecture

Five modules with clear separation of concerns:

1. **AbstractMediaMerger.py** — Abstract base class defining the 9-step merge pipeline. Defines `WriteStrategy` enum (DIRECT, PARTIAL_WITH_SIDECAR, VIDEO_WITH_SIDECAR), `MediaFileInfo` dataclass (includes pre-extracted `description` and `gps` fields to avoid shipping full `json_data` to workers, plus `existing_xmp_dates` for conditional date updates and `actual_ext` for extension mismatch handling), and `MergeStats` dataclass (with a `merge()` method for aggregating partial stats from parallel workers). Implements GPS resolution, duplicate filename resolution (appending `_2`, `_3`, etc.), dry-run logging, and summary reporting. Accepts a `num_workers` parameter (default 1); `_process_files` owns the serial-vs-parallel decision and writer lifecycle.

2. **GooglePhotosExportMerger.py** — Concrete implementation of AbstractMediaMerger and the CLI entry point. Builds ExifTool parameters for dates, descriptions, GPS, and timezones. Has a `blocked_descriptions` list in `__main__` for filtering unwanted descriptions. Implements parallel file processing via `ProcessPoolExecutor`: files are round-robin distributed across N worker processes, each with its own ExifTool instance. Core processing logic lives in shared module-level functions (`_do_process_matched`, `_do_process_orphan`, `_do_create_sidecar`, `_do_set_filesystem_timestamps`) that are used by both the serial class methods and the parallel worker. The parallel entry point `_process_chunk` configures worker logging and opens a per-worker ExifTool instance. To reduce IPC serialisation overhead, `description` and `gps` are pre-extracted onto `MediaFileInfo` and `json_data` is cleared before dispatch to workers.

3. **JsonFileIdentifier.py** — Matches JSON metadata files to their corresponding media files. Uses `SortedSet` for O(log n + k) prefix-based lookups. Handles Google's bracket notation (e.g., `filename(2).jpg`) and case-insensitive extension matching.

4. **JsonKeyExtractor.py** — Analysis entry point. Scans a directory tree once, groups files by directory, extracts JSON structure (2-level depth), and generates analysis output (combined_structure.json, individual_files.json, file_types.json, plus conditional error/conflict files).

5. **TestMerger.py** — Integration test suite (see Testing section above).

**Data flow:** JsonKeyExtractor scans directories → JsonFileIdentifier matches JSON-to-media files → GooglePhotosExportMerger writes metadata to EXIF.

## Key Design Details

- Write strategies: DIRECT (jpg, jpeg, tiff, tif, dng, cr2, heic), PARTIAL_WITH_SIDECAR (png, gif), VIDEO_WITH_SIDECAR (avi, mkv, mov, mp4, m4v, webm)
- QuickTime containers (mov, mp4, m4v) support direct tag writes (QuickTime:CreateDate, UserData:DateTimeOriginal, XMP tags); non-QuickTime video containers (avi, mkv, webm) are copy-only with all metadata in the XMP sidecar
- Metadata written: dates (with timezone), descriptions, GPS coordinates (latitude, longitude, altitude), timezone offsets
- Conditional date tag updates: pre-existing XMP/IPTC date tags (e.g. XMP-photoshop:DateCreated, IPTC:DateCreated) are updated to the resolved datetime only when they already exist in the source file — absent tags are left absent
- Sidecar fixup pass: after creating XMP sidecars via ExifTool's `-o`, a second in-place pass always force-writes the three core date tags (XMP-xmp:CreateDate, XMP-xmp:ModifyDate, XMP-exif:DateTimeOriginal) with the correct datetime and timezone. This is necessary because ExifTool's `-o` copies existing metadata from the source file (including non-XMP dates such as Nikon maker-note CreateDate) which can override the parameterised values without timezone
- Extension mismatch detection: ExifTool's `FileTypeExtension` is compared against the source file's extension. Equivalent extensions (jpg/jpeg, tif/tiff, mov/mp4/m4v/qt) are not flagged. True mismatches trigger a temporary rename during processing so ExifTool writes tags correctly, then the original extension is restored
- File creation and modified times are updated to match the photo/video date (FileCreateDate is Windows-only)
- Orphan files (no matching JSON) are still copied; dates resolved from existing EXIF or filesystem creation date
- Duplicate output filenames resolved by appending `_2`, `_3`, etc. (including renaming associated sidecars)
- `.gitignore` excludes all media and JSON files — only Python source is tracked

## Pipeline Steps

The 9-step merge pipeline defined in `AbstractMediaMerger.run()`:

1. **Validate directories** — check paths, create output directory
2. **Scan files** — walk the input tree, group by directory
3. **Match metadata to media** — pair JSON files with their media files
4. **Identify orphans** — find media files without matching JSON
5. **Resolve dates and output paths** — determine datetime (from JSON photoTakenTime + EXIF timezone), set write strategy, assign output paths (YYYY/MM/filename)
6. **Resolve duplicate filenames** — append `_2`, `_3` suffixes where needed
7. **Process matched files** — write metadata via ExifTool, create sidecars
8. **Process orphan files** — copy with optional date/description updates
9. **Log summary** — print stats

Steps 1–6 run serially using a single ExifTool instance (fast, involve shared state). Steps 7–8 run in parallel when `num_workers > 1`.

## Parallel Processing

- Steps 1–6 of the pipeline run serially using a single ExifTool instance — these are fast and involve shared state
- At the end of step 5, `description` and `gps` are pre-extracted from `json_data` onto `MediaFileInfo` fields, and `json_data` is set to `None` to reduce pickle/IPC overhead when dispatching to workers
- Steps 7–8 (file processing) run in parallel via `concurrent.futures.ProcessPoolExecutor` when `num_workers > 1`
- Each worker process configures its own logging handler and opens its own `ExifToolHelper` instance to avoid IPC bottlenecks
- Both serial and parallel paths share the same core processing functions (`_do_process_matched`, `_do_process_orphan`, `_do_create_sidecar`, `_do_set_filesystem_timestamps`) — the class methods are thin wrappers that delegate to these, and the parallel worker calls them directly
- Files are distributed round-robin across workers for balanced load
- Workers return partial `MergeStats` objects that are aggregated via `MergeStats.merge()` in the main process
- The `_process_files` method in `AbstractMediaMerger` owns the serial-vs-parallel decision and manages the writer lifecycle (opening/closing ExifTool for serial mode)
- `--workers 1` (or omitting `num_workers` from the constructor) preserves the original serial behaviour
- Dry-run mode always runs serially regardless of `num_workers`