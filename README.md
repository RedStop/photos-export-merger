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

Each media file in a Google Takeout export has a companion `.json` file containing metadata. The merger uses the following fields (all other fields are ignored):

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
| `title` | Yes | Used as the output filename. Must include the file extension (e.g. `IMG_1234.jpg`, `VID_001.MOV`). |
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
| `TestMerger.py` | Integration test suite (185+ tests across multiple configurations) |

**Data flow:** `JsonKeyExtractor` scans → `JsonFileIdentifier` matches JSON to media → `PhotosExportMerger` writes EXIF metadata.