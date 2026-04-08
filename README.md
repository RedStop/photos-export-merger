# Photos Export Merger

Merges JSON metadata from Photos Takeout exports into image/video EXIF properties using ExifTool. Two-phase workflow: **analyse** your export, then **merge** the metadata into your media files.

> **Disclaimer:** This project is designed to process data exported from Google Photos via Google Takeout. It is not affiliated with, endorsed by, sponsored by, or otherwise authorised by Google LLC. "Google", "Google Photos", and "Google Takeout" are trademarks of Google LLC. Use of these names herein is solely for the purpose of identifying the third-party service whose export format this tool supports.

**Back up your photos before running this.**

## Setup

### Quick start (recommended)

**Windows (PowerShell):** `.\setup.ps1`
**Linux / macOS / WSL:** `chmod +x setup.sh && ./setup.sh`

The script creates a `.venv` virtual environment, installs dependencies, and checks for ExifTool.

### Manual setup

1. Install **Python 3.10+** and create a virtual environment: `python -m venv .venv`
2. Activate it (`source .venv/bin/activate` or `.venv\Scripts\Activate.ps1`) and install packages: `pip install -r requirements.txt`
3. Install **ExifTool 12.45** from [exiftool.org](https://exiftool.org/) and ensure it's on your PATH.

Developed and tested on Windows. Linux/macOS/WSL work but `FileCreateDate` (filesystem creation time) is Windows-only — other platforms only set the modification time.

## Usage

### Phase 1: Analyse

Scan your export and review the reports before merging:

```bash
python JsonKeyExtractor.py <input_directory> [output_directory]
```

This generates `combined_structure.json`, `individual_files.json`, and `file_types.json`, plus conditional error files (`missing_files.json`, `duplicate_titles.json`, etc.) when issues are found. Fix any matching problems before proceeding.

### Phase 2: Merge

```bash
python PhotosExportMerger.py <input_dir> <output_dir> [options]
```

Run `python PhotosExportMerger.py --help` for full option details.

#### Examples

```bash
# Basic merge using all CPU cores
python PhotosExportMerger.py input/ output/

# Dry run — preview without writing files
python PhotosExportMerger.py input/ output/ --dry-run

# Single-threaded with fallback timezone set to UTC+2
python PhotosExportMerger.py input/ output/ --workers 1 --tz-fallback "+02:00"

# Strip Google camera metadata and override timezone for a trip
python PhotosExportMerger.py input/ output/ \
  --strip-metadata google \
  --tz-override "2019-11-20 02:00:00,2019-11-22 17:00:50,+05:30"

# Multiple timezone overrides for two trips
python PhotosExportMerger.py input/ output/ \
  --tz-override "2023-03-10 00:00:00,2023-03-20 23:59:59,+09:00" \
  --tz-override "2023-06-01 00:00:00,2023-06-15 23:59:59,-04:00"

# Recompress JPEGs above 80% quality
python PhotosExportMerger.py input/ output/ --jpeg-quality-threshold 80

# Recompress JPEGs but skip Lightroom and Darktable exports
python PhotosExportMerger.py input/ output/ --jpeg-quality-threshold 80 \
  --jpeg-quality-skip-editor lightroom --jpeg-quality-skip-editor darktable

# Skip compression for photos taken during a Japan trip (March 10–20, JST)
python PhotosExportMerger.py input/ output/ --jpeg-quality-threshold 80 \
  --jpeg-quality-skip-timerange "2023-03-10 00:00:00,2023-03-20 23:59:59,+09:00"

# List available editor software names
python PhotosExportMerger.py --list-editors

# Recompress JPEGs above 85% quality down to 75% quality
python PhotosExportMerger.py input/ output/ --jpeg-quality-threshold 85 --jpeg-target-quality 75

# Recompress at 65% quality, strip Google metadata, 4 workers
python PhotosExportMerger.py input/ output/ --jpeg-quality-threshold 65 --strip-metadata google --workers 4
```

### Testing

```bash
# Run the full test suite
python -m pytest TestMerger.py

# Run with verbose output
python -m pytest TestMerger.py -v

# Run a specific test class
python -m pytest TestMerger.py -k "TestTimezoneOverride"

# Run a single test
python -m pytest TestMerger.py -k "test_timezone_gmt_plus_2"
```

The custom test runner (`python TestMerger.py`) supports additional options:

```bash
# List available test classes
python TestMerger.py --list-classes

# Run specific test classes (case-insensitive substring match; repeatable)
python TestMerger.py --class TestPhotosExportMerger --class TestJpegCompressionWithFullTree

# Combine with category and file-type filters
python TestMerger.py --class TestPhotosExportMerger -c GPS -t jpg

# Include single-worker regression tests
python TestMerger.py --single-worker

# List available categories and file types
python TestMerger.py --list-categories
python TestMerger.py --list-types
```

---

## How the merger works

The merger copies files to a date-organized output directory (`YYYY/MM/filename`) and writes metadata from the JSON sidecar files into the media files' EXIF tags.

**Date resolution:** For matched files, the date comes from the JSON `photoTakenTime` timestamp combined with the EXIF timezone offset. For orphans (no JSON), dates come from existing EXIF tags or the filesystem creation date. When no EXIF timezone is found, `--tz-override` ranges are checked before falling back to the fallback timezone (`--tz-fallback`, or the host machine's local timezone by default).

**Metadata written:** dates with timezone, descriptions, GPS coordinates, timezone offsets, and file timestamps. Pre-existing XMP/IPTC date tags are updated only when they already exist in the source file — absent tags are left absent.

**Write strategies** depend on file type:

| Category | Extensions | Approach |
|----------|------------|----------|
| Full EXIF write | `.jpg`, `.jpeg`, `.tiff`, `.tif`, `.dng`, `.cr2`, `.heic` | Tags written directly |
| Partial write + sidecar | `.png`, `.gif` | XMP tags + `.xmp` sidecar |
| QuickTime video + sidecar | `.mov`, `.mp4`, `.m4v` | QT/XMP tags + `.xmp` sidecar |
| Copy-only + sidecar | `.avi`, `.mkv`, `.webm` | File copied; all metadata in `.xmp` sidecar |

**Input directory structure:** The merger scans up to **2 directory levels** inside `<input_dir>`. Files at greater depth are silently skipped. This matches the layout Google Photos Takeout produces — typically a single level of album subdirectories, each containing media files and their companion `.json` files.

**Parallel processing:** Steps 1–6 (scanning, matching, date resolution) run serially. Steps 7–8 (file processing) run in parallel across `--workers` processes, each with its own ExifTool instance.

### Timezone fallback and overrides

When a file has no embedded EXIF timezone (common for videos and some cameras), the merger uses the fallback timezone. Set it explicitly with `--tz-fallback "+02:00"` or leave it to default to your machine's local timezone.

For travel photos, use `--tz-override` to specify UTC time ranges and the timezone to apply. Files with an embedded EXIF timezone are never affected — EXIF always takes priority. When multiple overrides are specified, the first matching range wins.

### Metadata stripping

`--strip-metadata` removes unwanted metadata groups after writing. Non-QuickTime video containers are skipped (ExifTool can't modify them in-place). Only files that actually contain the targeted metadata are processed.

| Profile | Removes |
|---------|---------|
| `google` | `XMP-GCamera:All`, `Google:All` |
| `photoshop` | `Photoshop:All`, `XMP-photoshop:DocumentAncestors` |

Use `--strip-metadata` (no args) for all profiles, or name specific ones: `--strip-metadata google photoshop`. Add new profiles to `METADATA_STRIP_PROFILES` in `PhotosExportMerger.py`.

### JPEG compression

`--jpeg-quality-threshold PERCENT` recompresses JPEG images whose estimated quality exceeds the given threshold (1–100). JPEGs at or below the threshold are copied as-is. The quality is estimated via ExifTool's `File:JPEGQualityEstimate` during the scan phase; files whose quality cannot be determined are conservatively recompressed.

`--jpeg-target-quality PERCENT` sets the target output quality (1–100) for recompressed JPEGs. When omitted, images are compressed to the `--jpeg-quality-threshold` value. This allows separate control over the decision threshold and the output quality — for example, `--jpeg-quality-threshold 85 --jpeg-target-quality 75` recompresses any JPEG above 85% quality down to 75%. A warning is printed if used without `--jpeg-quality-threshold`.

Compression uses Pillow in memory — the compressed bytes are piped directly into ExifTool via stdin, which copies all metadata from the original source (`-TagsFromFile`) and applies tag modifications in one pass, with no intermediate file written to disk. As a safety net, if the compressed output is not smaller than the original file, the original image is used instead (logged as `SKIP-COMPRESS`). Metadata stripping (`--strip-metadata`) runs as a separate pass afterward. Applies to both matched and orphan files. Only files with JPEG extensions (`.jpg`, `.jpeg`, `.jpe`, `.jfif`) are eligible.

#### Skipping editor-exported images

`--jpeg-quality-skip-editor NAME` excludes JPEGs exported from named photo editing software from recompression. The rationale is that professional editors (Lightroom, Darktable, etc.) already export at an intentionally chosen quality. Detection uses the `EXIF:Software` and `XMP-xmp:CreatorTool` tags via case-insensitive substring matching.

The option is repeatable — specify multiple editors to skip:

```bash
python PhotosExportMerger.py input/ output/ --jpeg-quality-threshold 80 \
  --jpeg-quality-skip-editor lightroom --jpeg-quality-skip-editor darktable
```

Use `--list-editors` to see all available editors and their match patterns. Editor names support case-insensitive substring matching (e.g. `light` matches `lightroom`). The special name `all` skips all known editors. A warning is printed if `--jpeg-quality-skip-editor` is used without `--jpeg-quality-threshold`.

Add new editors to `EDITOR_SOFTWARE_PATTERNS` in `PhotosExportMerger.py` — they will automatically appear in `--list-editors`.

#### Skipping images by time range

`--jpeg-quality-skip-timerange "START,END,OFFSET"` excludes JPEGs whose resolved datetime falls within the given time range from recompression. The format is identical to `--tz-override`: the start and end datetimes are in the timezone specified by the third field, and are converted to UTC internally for comparison.

The option is repeatable — specify multiple ranges to skip:

```bash
python PhotosExportMerger.py input/ output/ --jpeg-quality-threshold 80 \
  --jpeg-quality-skip-timerange "2023-03-10 00:00:00,2023-03-20 23:59:59,+09:00" \
  --jpeg-quality-skip-timerange "2023-06-01 00:00:00,2023-06-15 23:59:59,-04:00"
```

Time-range exclusion takes **precedence over** editor exclusion: if a file matches both a time range and an editor pattern, only the `jpeg_compress_skipped_timerange` counter is incremented (not `jpeg_compress_skipped_editor`). A warning is printed if used without `--jpeg-quality-threshold`.

### Blocking unwanted descriptions

Edit the `blocked_descriptions` list in the `__main__` block of `PhotosExportMerger.py`:

```python
blocked_descriptions = [
    "SONY DSC",
    "OLYMPUS DIGITAL CAMERA",
]
```

Blocked descriptions are cleared from `EXIF:UserComment`, `EXIF:ImageDescription`, `XMP-dc:Description`, and `IPTC:Caption-Abstract` (if present).

## JSON metadata format

Each media file in a Google Takeout export has a companion `.json` file containing metadata. The filename of the `.json` file is used to find the matching media file using the following cascade:

1. **Strip the `.json` extension** to get the expected media filename (e.g. `IMG_1234.jpg.json` → `IMG_1234.jpg`).
2. **Handle bracket duplicates.** Google Takeout appends `(N)` after the extension for duplicates (e.g. `IMG_1234.jpg(2).json`). The matcher detects this, strips the bracket suffix, and reinserts it before the extension to reconstruct the expected filename (`IMG_1234(2).jpg`).
3. **Try an exact match** against the files in the same directory.
4. **Fall back to a prefix search.** If no exact match is found, all files in the directory whose name starts with the JSON base (without any bracket suffix) and whose extension matches the `title` field's extension are collected. If exactly one candidate exists, it is used. If multiple candidates exist, the one whose `(N)` bracket number matches the JSON's bracket number is selected.

Files that have no matching JSON are flagged as **orphans** and copied to the output with their existing EXIF metadata preserved.

The merger uses the following JSON fields (all other fields are ignored):

```json
{
  "title": "PXL_20200808_180006041.jpg",
  "description": "Some fancy description with special chars like é, ô and even 郭.\nOh, and new lines are also supported!",
  "photoTakenTime": {
    "timestamp": "1723113846"
  },
  "geoData": {
    "latitude": 13.8121437,
    "longitude": 21.6436809,
    "altitude": 30.2
  },
  "geoDataExif": {
    "latitude": 13.8121437,
    "longitude": 21.6436809,
    "altitude": 30.2
  }
}
```

| Field | Required | Usage |
|-------|----------|-------|
| `title` | Yes | Used as the output filename, with the extention always converted to lowercase. Must include the file extension (e.g. `IMG_1234.jpg`, `VID_001.MOV`). |
| `description` | No | Written to `EXIF:ImageDescription`, `XMP-dc:Description`, and `IPTC:Caption-Abstract` (if already present). Supports UTF-8 and newlines. |
| `photoTakenTime.timestamp` | Yes | Unix epoch (seconds since 1970-01-01 UTC). Combined with the EXIF timezone offset to produce the local datetime used for date tags and the `YYYY/MM/` output directory structure. |
| `geoData` | No | GPS coordinates written to EXIF and XMP GPS tags. Ignored when both latitude and longitude are `0.0`. |
| `geoDataExif` | No | Fallback GPS source — used only when `geoData` has no valid coordinates. Same format as `geoData`. |

## Architecture

| Module | Role |
|--------|------|
| `AbstractMediaMerger.py` | Base class: 9-step pipeline, dataclasses, GPS/duplicate resolution |
| `PhotosExportMerger.py` | Concrete implementation, ExifTool integration, CLI, parallel processing |
| `JsonFileIdentifier.py` | Matches JSON metadata files to media files |
| `JsonKeyExtractor.py` | Analysis tool — scans exports and generates structural reports |
| `TestMerger.py` | Integration test suite (420+ tests across multiple configurations) |

**Data flow:** `JsonKeyExtractor` scans → `JsonFileIdentifier` matches JSON to media → `PhotosExportMerger` writes EXIF metadata.