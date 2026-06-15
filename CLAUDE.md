# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Photos Export Merger — a Python utility that merges JSON metadata from Photos Takeout exports into image/video EXIF properties using ExifTool. Developed and tested on Windows; setup scripts also support Linux/macOS/WSL, though the `FileCreateDate` tag used for filesystem creation times is Windows-only (on other platforms only the modification time is reliably set).

## Requirements

- Python 3.10.11 (virtual environment in `.venv/`)
- ExifTool 12.45 (must be in system PATH)
- PyExifTool 0.5.6, sortedcontainers 2.4.0, Pillow

## Running

```bash
# Analyse JSON metadata structure across a directory tree
python JsonKeyExtractor.py <input_directory> [output_directory]

# Merge metadata into media files (uses all CPU cores by default)
python PhotosExportMerger.py <input_dir> <output_dir> [--dry-run] [--workers N] [--strip-metadata [PROFILE ...]] [--tz-fallback OFFSET] [--tz-override "START_UTC,END_UTC,OFFSET" ...] [--jpeg-quality-threshold PERCENT] [--jpeg-target-quality PERCENT] [--jpeg-quality-skip-editor NAME ...] [--jpeg-quality-skip-timerange "START,END,OFFSET" ...] [--list-editors]

# Run tests (all classes via pytest)
python -m pytest TestMerger.py

# Run tests (custom runner with class selection)
python TestMerger.py --class TestPhotosExportMerger --class TestJpegCompressionWithFullTree
python TestMerger.py --list-classes

# Run reencode_av1 (requires ffmpeg and ffprobe on PATH)
python -m reencode_av1                              # current directory
python -m reencode_av1 /path/to/videos              # specific directory
python -m reencode_av1 /path/to/clip.mp4            # a single video file
python -m reencode_av1 --target-bitrate 2000        # lower target
python -m reencode_av1 --dry-run                    # preview only
python -m reencode_av1 --search-method binary       # pure binary search (default is smart)
python -m reencode_av1 --search-method interpolation # CRF interpolation (default is smart)
python -m reencode_av1 --no-precise                 # skip the full-video re-search (precise is on by default)
python -m reencode_av1 --precise-only               # search on the full video from the start (no sampling)
python -m reencode_av1 --no-progress                # ignore progress files (don't read or write them)
python -m reencode_av1 --scratch-dir D:/scratch //nas/videos  # stage NAS sources on local disk

# Run reencode_av1 tests
python -m pytest TestReencodeAv1.py -v
```

## Testing

`TestMerger.py` is a comprehensive `unittest`-based test suite with 185+ test methods (including subtests). It runs as a single-pass integration test: `setUpClass` builds an input tree with programmatically generated binary test files (JPEG, PNG, GIF, TIFF, CR2, DNG, HEIC, MP4, MOV, AVI, MKV, WebM — including variants with embedded EXIF timezone offsets and Nikon maker-note dates), runs the merger once, then individual tests assert on the output. Test categories include: input integrity, output structure, GPS (8 compass directions × 12 formats), timezones (including sidecar timezone verification), descriptions (UTF-8, escaping, newlines, blocked, IPTC), file types, orphan files, XMP conditional dates, XMP sidecars, duplicates, bracket notation, file timestamps, stats verification, video UTC time, special filenames, EXIF preservation, extension mismatch, video XMP dates (including Nikon maker-note sidecar fixup), metadata stripping (profile building and default-off verification), infrastructure validation, and single-worker (serial) mode.

Separate test classes exercise features that require different merger configurations: `TestMetadataStripping` (runs with strip params enabled), `TestTimezoneOverride` (runs with `--tz-override` ranges to verify override/fallback/EXIF-priority behaviour for matched files, orphans, and sidecars), `TestFallbackTimezone` (runs with a non-default `--tz-fallback` to verify the custom fallback timezone is applied to files without EXIF timezone), `TestJpegCompression` (runs with `--jpeg-quality-threshold` to verify JPEG recompression), `TestJpegSkipLightroom` and `TestJpegSkipDarktable` (inherit shared `_JpegSkipBase` which creates editor-tagged JPEGs at high and low quality plus time-range test JPEGs with distinct timestamps; each subclass excludes a different editor via `--jpeg-quality-skip-editor` and a July 2024 time range via `--jpeg-quality-skip-timerange`, verifying that excluded editors' and in-range files are NOT compressed while non-excluded files ARE, and that time-range exclusion takes precedence over editor exclusion), `TestJpegTargetQuality` (runs with separate `--jpeg-quality-threshold` and `--jpeg-target-quality` to verify the output quality matches the target rather than the threshold), `TestJpegCompressionWithFullTree` (inherits `TestPhotosExportMerger` and re-runs the full test tree with JPEG compression enabled), and `TestSingleWorker` (re-runs with `num_workers=1`).

The custom test runner (`python TestMerger.py`) runs all classes by default except `TestSingleWorker` (opt-in via `--single-worker`). Use `--class NAME` (repeatable, case-insensitive substring match) to run specific classes, or `--list-classes` to see all available classes. Category (`-c`) and file-type (`-t`) filters can be combined with `--class`.

## Standalone Scripts

- **reencode-av1.ps1** — PowerShell script for batch re-encoding videos to AV1 (libsvtav1) with automatic CRF tuning. Recursively finds videos in the current directory, binary-searches CRF values by encoding a 10-second sample to find one that produces a bitrate in the acceptable range (default: 2000–2500 kbit/s), then encodes the full video. Skips videos already encoded as AV1 or VP9. Downscales videos above 1080p (never upscales). Outputs .mkv files with Opus audio. Requires ffmpeg and ffprobe on PATH. Run `.\reencode-av1.ps1 -Help` for full usage.

- **reencode_av1/** — Python package equivalent of reencode-av1.ps1 (stdlib only; needs ffmpeg/ffprobe on PATH). Run `python -m reencode_av1 [path]` (file or directory, defaults to cwd); see `--help` for full usage. Adds over the PowerShell script: multi-segment sampling (5 segments via concat filter), selectable CRF search via `--search-method {smart,interpolation,binary}` (smart default), precise mode (on by default — re-searches on full-video encodes if the final bitrate is out of range, reusing the pre-search full encode as a seed; `--no-precise` keeps the first sample encode, `--precise-only` searches the full video from the start), configurable audio bitrate, local scratch staging (`--scratch-dir`, copies sources local before encoding then moves output to its destination), fixed-CRF mode (`--crf-min == --crf-max` pins a CRF and skips the whole search; bitrate/window/search/precise/ceiling options ignored, skips still apply), and per-folder progress tracking. Targets, `--skip-below-bitrate`, and acceptance windows are all whole-file (total = video + audio + container overhead), so the final file lands near the target; the smart/interpolation search measures the audio+container overhead per phase and removes it before the log-linear CRF estimate to keep the estimate accurate (see `search.py` below).

  Per-folder progress tracking: each folder with videos gets a `reencode-av1-progress.json` recording each video's outcome (and final CRF for encodes). On later runs, videos already listed are dropped before processing (never re-probed/encoded). The progress file always lives in the real folder (never staged via `--scratch-dir`). `process_file` returns a `FileResult` (status + final CRF); the main loop records all outcomes except failures (retried) and dry-runs. `--no-progress` disables reading and writing entirely.

  Package structure:
  - `__main__.py` — CLI parsing, validation, main loop (defines `FileResult`)
  - `encode.py` — FFmpeg sample/segment/full encode helpers
  - `filters.py` — scaling, GOP, bitrate window, segment offset helpers
  - `probe.py` — ffprobe utilities (`VideoInfo` incl. `total_bitrate_kbps`, `get_total_bitrate`, `measure_overhead`, `_sum_video_packet_bytes`)
  - `progress.py` — progress tracking (`load_progress`, `record_progress`, `progress_path_for`, `PROGRESS_FILENAME`)
  - `search.py` — outer CRF search loop (`find_optimal_crf`) with pluggable stateless methods (`smart_search_next` default, `binary_search_next`, `interpolation_next`). CRF/bitrate pairs are `CrfPoint(crf, bitrate)` instances where `bitrate` is the **total** (whole-file) bitrate (the two fields move oppositely). The outer loop picks the next CRF, nudges ±1 on a duplicate, encodes, then checks the accept/confident window, the `--crf-max` ceiling (returns `crf-ceiling-fallback` with `crf_ceiling_used=True`), and the `--crf-min` floor. Accepts `seed_known` points, `seed_temp_files` (their already-encoded files, sharing the in-loop temp-file lifecycle), and `seed_overhead`. All methods share a single `SearchContext(crf_min, crf_max, accept_lo, accept_hi, confident_lo, confident_hi, seed_crf, overhead)`; the confident bounds are used only by smart search and `overhead` only by the interpolating methods. Smart search assumes monotonicity, targets the confident-window centre, probes the `[crf_min, crf_max]` midpoint, jumps to a bound to bracket, then log-linearly interpolates the straddling pair until consecutive.

    Overhead-aware interpolation: the outer loop works in total-bitrate space (every recorded `CrfPoint.bitrate` and every window/acceptance check is whole-file, so the final audio+video+overhead lands near the target), while the search methods interpolate in video space. Only the video component is log-linear in CRF, so `interpolate_crf(..., overhead)` subtracts the estimated audio+container `overhead` from both points and the target before the log fit (overhead 0 ⇒ plain log-linear; non-positive video ⇒ midpoint fallback). The overhead is *measured* (not assumed) per search phase: `find_optimal_crf` requests it (`measure_overhead=True`) on the first completed encode of the call, caches it, and reuses it for the rest of that phase. Partial (segment-sample) and full-video phases each measure their own; for the precise re-search, `__main__` measures the overhead of the pre-search full encode and passes it as `seed_overhead` so the precise loop never re-derives it from a large file. `measure_overhead` is `total − video`, with `total` from the container/file size and `video` from summing video packet sizes (reliable for MKV, which usually omits per-stream bitrates); no-audio sources just yield a small container-only overhead.

## Testing — reencode_av1

`TestReencodeAv1.py` is a pytest-based test suite covering all modules of the reencode_av1 package. Tests use `unittest.mock` to mock ffprobe/ffmpeg calls, so no actual video files or encoding tools are required to run them.

Covers all modules: probe helpers (`_parse_fraction`, `get_video_info` incl. `total_bitrate_kbps`, `get_total_bitrate`, `measure_overhead`), filter helpers (`_get_scale_filter`, `build_extra_args`, `compute_segment_offsets`, `compute_windows` plus parametrized window invariants), encode helpers (`_base_encode_args`, `_extract_vf_filter`, `_parse_time_to_seconds`), the search methods (`interpolate_crf` incl. overhead, `binary_search_next`, `interpolation_next`, `smart_search_next` — all via `SearchContext`) and the `find_optimal_crf` outer loop (confident-zone exit, ceiling/floor, seeded search, duplicate nudging, temp-file preservation/reuse, encode failures, overhead measured-once-then-cached, `seed_overhead`), `get_output_path`, `compute_audio_bitrate`, `validate_args` (path validation, `--precise-only`/`--no-precise` conflict, `--scratch-dir`, fixed-CRF), `build_parser` (defaults and parsing), `process_file` (skips, dry run, temp cleanup, `--precise-only`, `--scratch-dir` staging, fixed-CRF), and progress tracking (`load_progress`/`record_progress` round-trip, preservation, overwrite, malformed-file handling).

## Architecture

Five modules with clear separation of concerns, plus a standalone video re-encoding package:

1. **AbstractMediaMerger.py** — Abstract base class defining the 9-step merge pipeline. Defines `WriteStrategy` enum (DIRECT, PARTIAL_WITH_SIDECAR, VIDEO_WITH_SIDECAR), `MediaFileInfo` dataclass (includes pre-extracted `description` and `gps` fields to avoid shipping full `json_data` to workers, plus `existing_xmp_dates` for conditional date updates and `actual_ext` for extension mismatch handling), and `MergeStats` dataclass (with a `merge()` method for aggregating partial stats from parallel workers). Implements GPS resolution, duplicate filename resolution (appending `_2`, `_3`, etc.), dry-run logging, and summary reporting. Accepts a `num_workers` parameter (default 1); `_process_files` owns the serial-vs-parallel decision and writer lifecycle.

2. **PhotosExportMerger.py** — Concrete implementation of AbstractMediaMerger and the CLI entry point. Builds ExifTool parameters for dates, descriptions, GPS, and timezones. Has a `blocked_descriptions` list in `__main__` for filtering unwanted descriptions. Implements parallel file processing via `ProcessPoolExecutor`: files are round-robin distributed across N worker processes, each with its own ExifTool instance. Core processing logic lives in shared module-level functions (`_do_process_matched`, `_do_process_orphan`, `_do_create_sidecar`, `_do_set_filesystem_timestamps`) that are used by both the serial class methods and the parallel worker. The parallel entry point `_process_chunk` configures worker logging and opens a per-worker ExifTool instance. To reduce IPC serialisation overhead, `description` and `gps` are pre-extracted onto `MediaFileInfo` and `json_data` is cleared before dispatch to workers.

3. **JsonFileIdentifier.py** — Matches JSON metadata files to their corresponding media files. Uses `SortedSet` for O(log n + k) prefix-based lookups. Handles Takeout bracket notation (e.g., `filename(2).jpg`) and case-insensitive extension matching.

4. **JsonKeyExtractor.py** — Analysis entry point. Scans a directory tree once, groups files by directory, extracts JSON structure (2-level depth), and generates analysis output (combined_structure.json, individual_files.json, file_types.json, plus conditional error/conflict files).

5. **TestMerger.py** — Integration test suite (see Testing section above).

6. **reencode_av1/** — Python package for batch AV1 re-encoding with automatic CRF tuning (see Standalone Scripts section above).

7. **TestReencodeAv1.py** — pytest test suite for reencode_av1 (see Testing — reencode_av1 section above).

**Data flow:** JsonKeyExtractor scans directories → JsonFileIdentifier matches JSON-to-media files → PhotosExportMerger writes metadata to EXIF.

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
- Metadata stripping (`--strip-metadata`): optional post-write ExifTool pass that removes unwanted metadata groups from output files. Controlled by named profiles defined in `METADATA_STRIP_PROFILES` (currently `google` and `photoshop`). The special name `all` enables every profile. Strip params are stored on `MediaFileInfo.strip_metadata_params` so they are available to parallel workers. Non-QuickTime video containers are skipped (ExifTool cannot modify them in-place)
- Timezone fallback (`--tz-fallback`): sets the fallback timezone offset used when no EXIF timezone is found and no `--tz-override` matches. Defaults to the host machine's local timezone if not specified. Stored on `self.fallback_tz` and propagated to parallel workers via `MediaFileInfo.fallback_tz`
- Timezone overrides (`--tz-override`): repeatable option that specifies UTC time ranges and a timezone offset. When a file has no EXIF timezone and its UTC timestamp falls within an override range, the override timezone is used instead of the fallback timezone. Defined via `TimezoneOverride` dataclass in `AbstractMediaMerger.py`. `_find_tz_override()` does linear scan of overrides (first match wins). Applies to both matched and orphan files. For orphans resolved from EXIF dates (which are naive/local), the parsed datetime is treated as a UTC approximation for range matching
- JPEG compression (`--jpeg-quality-threshold`): optional recompression of JPEG images whose ExifTool `File:JPEGQualityEstimate` exceeds the configured threshold (1-100, default: disabled). Quality estimates are read during the batch EXIF scan in step 5 and stored on `MediaFileInfo.jpeg_quality`. During processing (steps 7-8), qualifying JPEGs are compressed with Pillow in memory (no metadata transferred), then the compressed bytes are piped via stdin to a standalone `exiftool` subprocess that copies all metadata from the original source (`-TagsFromFile`, `-All:All`) and applies tag modifications — all in a single invocation with zero intermediate disk writes. The piping approach works on both Windows and Linux. Only files with actual JPEG extensions (.jpg, .jpeg, .jpe, .jfif) are eligible — extension-mismatched files (e.g. JPEG content with .dng extension) are not compressed. Files whose quality could not be determined are conservatively recompressed. A size guard prevents accidental file-size increases: if the compressed bytes are >= the original file size, the original image bytes are piped through instead and the `jpeg_compress_skipped_larger` counter is incremented. Metadata stripping (`--strip-metadata`) runs as a separate pass after compression. Stats counters tracked: `jpeg_compressed` (files recompressed), `jpeg_quality_unknown` (files where ExifTool could not estimate quality), `jpeg_quality_checked` (JPEG files scanned for quality), and `jpeg_compress_skipped_larger` (files where compression was discarded because it didn't reduce size). Applies to both matched and orphan files. The `jpeg_compress_quality` threshold is propagated to workers via `MediaFileInfo.jpeg_compress_quality`. `--jpeg-target-quality PERCENT` optionally overrides the Pillow output quality (default: threshold value), stored on `MediaFileInfo.jpeg_target_quality` and propagated to workers; when set, `_compress_and_write_jpeg` uses `jpeg_target_quality` instead of `jpeg_compress_quality` for Pillow's save quality
- Editor skip (`--jpeg-quality-skip-editor`): repeatable option that excludes JPEGs exported from named editing software from `--jpeg-quality-threshold` recompression. Detection uses case-insensitive substring matching of `EXIF:Software` and `XMP-xmp:CreatorTool` tags against patterns in `EDITOR_SOFTWARE_PATTERNS` (defined in `PhotosExportMerger.py`). Each pattern dict has `match` substrings (any must be present) and optional `exclude` substrings (none may be present) for precise disambiguation (e.g. `photoshop` matches "Adobe Photoshop CC" but not "Adobe Photoshop Lightroom"). CLI supports `--list-editors` to print available editors, `all` keyword to select all editors, and case-insensitive substring matching of editor names (same as `--class` in TestMerger.py). Tags are read during the batch EXIF scan in step 5 and the result is stored as `MediaFileInfo.jpeg_skip_editor` (bool). Warns if used without `--jpeg-quality-threshold`. Stats counter: `jpeg_compress_skipped_editor`
- Time-range skip (`--jpeg-quality-skip-timerange`): repeatable option that excludes JPEGs from `--jpeg-quality-threshold` recompression when their resolved datetime falls within a specified time range. Uses the same 3-part format as `--tz-override` (`"YYYY-MM-DD HH:MM:SS,YYYY-MM-DD HH:MM:SS,+HH:MM"`), where start/end are in the given timezone and converted to UTC internally. Defined via `JpegSkipTimerange` dataclass in `AbstractMediaMerger.py`. `_check_jpeg_skip_timerange()` converts the file's `resolved_datetime` to UTC and checks against all ranges (first match wins). The check runs in step 5 after `resolved_datetime` is set. Takes **precedence over** editor skip: in `_needs_jpeg_compression()` and the counter logic in `_do_process_matched`/`_do_process_orphan`, time-range is checked before editor. Result stored as `MediaFileInfo.jpeg_skip_timerange` (bool). Warns if used without `--jpeg-quality-threshold`. Stats counter: `jpeg_compress_skipped_timerange`
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