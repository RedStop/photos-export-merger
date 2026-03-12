# Google Photos Export Merger

A Python utility that merges JSON metadata from Google Photos Takeout exports into image/video EXIF properties using ExifTool. This is a two-phase workflow: first **analyse** your export to identify issues, then **merge** the metadata into your media files.

No guarantees are made, so ensure you have a backup of your photos somewhere else before running this script.

## Quick Start

### Windows (PowerShell)
```powershell
.\setup.ps1
```

### Linux / macOS / WSL (Bash)
```bash
chmod +x setup.sh
./setup.sh
```

The setup script will:
1. Verify Python is installed
2. Create a `.venv` virtual environment
3. Install Python dependencies from `requirements.txt`
4. Check for ExifTool and attempt to download/install it if missing

## Manual Setup

If you prefer to set up manually or the setup script doesn't work for your environment:

1. **Python 3.10+** — install from [python.org](https://www.python.org/)
2. **Create and activate a virtual environment:**
   ```bash
   python -m venv .venv
   # Windows PowerShell:
   .venv\Scripts\Activate.ps1
   # Linux/macOS:
   source .venv/bin/activate
   ```
3. **Install Python packages:**
   ```bash
   pip install -r requirements.txt
   ```
4. **ExifTool 12.45** — download from [exiftool.org](https://exiftool.org/):
   - Place `exiftool.exe` in the project folder, **or**
   - Install it to a directory on your system PATH

## Requirements

- Python 3.10.11
- [ExifTool](https://exiftool.org/) 12.45 (should be available in the system PATH) — I used the Windows executable.
- Developed and tested on Windows. Setup scripts also support Linux/macOS/WSL, though the `FileCreateDate` tag used for setting filesystem creation times is a Windows-only ExifTool feature (on other platforms only the modification time is reliably set).

## Python Packages

- PyExifTool 0.5.6 (`pip install PyExifTool`)
- sortedcontainers 2.4.0 (`pip install sortedcontainers`)

---

## Phase 1: Analyse with JsonKeyExtractor.py

Run the analyser to scan your Google Photos Takeout export and generate reports about the JSON metadata structure, file matching, and potential issues.

```bash
python JsonKeyExtractor.py <input_directory> [output_directory]
```

- `input_directory` — the root of your Google Photos Takeout export.
- `output_directory` — where to write the analysis files (defaults to `output`).

### Generated output files

The analyser always produces:

| File | Description |
|------|-------------|
| `combined_structure.json` | Summary of all JSON field names and types (2 levels deep). Useful for understanding the schema of Google Photos JSON metadata. |
| `individual_files.json` | Per-JSON-file details: matching media filename, title, and structure. |
| `file_types.json` | Summary count of each file extension, plus detailed listing for non-common types. Use this to check all media types are supported. |

The following files are only generated when issues are found:

| File | Description |
|------|-------------|
| `type_conflicts.json` | Type inconsistencies across JSON files (e.g. a field is a string in one file and a dict in another). |
| `duplicate_titles.json` | Multiple JSON files with the same title in the same folder. |
| `duplicate_matching_filenames.json` | Multiple JSON files pointing to the same media file. |
| `missing_files.json` | JSON files that don't match any media file in the same directory. |
| `unreferenced_files.json` | Media files with no matching JSON metadata. |
| `descriptions.json` | Non-empty descriptions found in JSON files. |

---

## Manual Data Cleanup

After running the analyser, review and fix issues **before** running the merger.

### Review and fix matching problems

- **`missing_files.json`** — Rename media files or edit JSON filename so they match correctly.
- **`duplicate_matching_filenames.json`** — Resolve duplicates by renaming files.
- **`duplicate_titles.json`** — Resolve duplicates by renaming titles in the JSON files.
- **`type_conflicts.json`** — Investigate unexpected type differences across your JSON files.
- **`unreferenced_files.json`** — Investigate if there is truly no JSON file for this media file. (There doesn't have to be a JSON file.)

### Review descriptions

- **`descriptions.json`** — Check for incorrect or unwanted descriptions. Common junk descriptions (e.g. `"SONY DSC"`, `"OLYMPUS DIGITAL CAMERA"`) can be blocked in the merger's `blocked_descriptions` list (see Phase 2).

### Check supported file types

Review `file_types.json` and verify your media types are supported. Unsupported file types are skipped with a warning and will **not** be copied to the output.

| Category | Extensions |
|----------|------------|
| **Full EXIF write** | `.jpg`, `.jpeg`, `.tiff`, `.tif`, `.dng`, `.cr2`, `.heic` |
| **Partial write + XMP sidecar** | `.png`, `.gif` |
| **Video (copy + XMP sidecar)** | `.avi`, `.mkv`, `.mov`, `.mp4`, `.m4v`, `.webm` |

For video files, QuickTime containers (`.mov`, `.mp4`, `.m4v`) also support direct tag writes (QuickTime dates, UserData, XMP tags) in addition to the sidecar. Non-QuickTime video containers (`.avi`, `.mkv`, `.webm`) are copy-only — all metadata lives in the XMP sidecar.

---

## Phase 2: Merge with GooglePhotosExportMerger.py

Once your data is clean, run the merger to write JSON metadata into your media files' EXIF tags and copy everything into a date-organized output directory.

### Before you start

- **Back up your data.** The merger copies files to a new output directory but you should always have a separate backup.
- **Run the analyser first** and fix any issues (see above).
- The output directory must **not** be inside the input directory, or vice versa.

### Usage

```bash
python GooglePhotosExportMerger.py <input_dir> <output_dir> [--dry-run] [--workers N]
```

- `input_dir` — The root of your Google Photos Takeout export.
- `output_dir` — Where to write the merged output. Will be created if it doesn't exist.
- `--dry-run` — Simulate the merge without writing any files. Useful for previewing what would happen.
- `--workers N` — Number of parallel worker processes for file processing. Defaults to the number of CPU cores. Each worker runs its own ExifTool instance. Use `--workers 1` to force single-process (serial) mode.

### What the merger does

The merger follows a 9-step pipeline:

1. **Validate directories** — checks paths, creates the output directory.
2. **Scan files** — walks the input tree and groups files by directory.
3. **Match metadata to media** — pairs each JSON metadata file to its corresponding media file using `JsonFileIdentifier`.
4. **Identify orphans** — finds media files with no matching JSON.
5. **Resolve dates and output paths** — determines the datetime for each file from the JSON `photoTakenTime` timestamp combined with the EXIF timezone offset (falls back to GMT+02:00 if no timezone is found). Assigns a write strategy and output path (`YYYY/MM/filename`).
6. **Resolve duplicate filenames** — appends `_2`, `_3`, etc. when multiple files resolve to the same output name (including renaming associated sidecars).
7. **Process matched files** — writes metadata (dates with timezone, descriptions, GPS coordinates) into EXIF tags using ExifTool. Creates XMP sidecars where needed.
8. **Process orphan files** — copies to the output directory. Dates are resolved from existing EXIF tags or the filesystem creation date.
9. **Log summary** — prints stats.

Steps 1–6 run serially using a single ExifTool instance (these are fast and involve shared state). Steps 7–8 run in parallel across multiple worker processes when `--workers` is greater than 1, with each worker managing its own ExifTool instance. Both serial and parallel paths share the same core processing logic — the class methods delegate to shared module-level functions that are also used by the parallel workers.

### Metadata written

- **Dates** — `DateTimeOriginal`, `CreateDate`, `ModifyDate` with timezone offset. For QuickTime videos, UTC dates are also written to QuickTime-specific tags.
- **Descriptions** — written to `XMP-dc:Description` and `EXIF:ImageDescription`. If the source file already has `IPTC:Caption-Abstract`, that tag is also updated. Descriptions containing only whitespace are automatically skipped.
- **GPS** — latitude, longitude, and altitude written to EXIF GPS tags (and XMP GPS for sidecar formats). GPS coordinates of (0, 0) are treated as absent.
- **Timezone offsets** — `OffsetTime`, `OffsetTimeOriginal`, `OffsetTimeDigitized` for EXIF-writable formats.
- **File timestamps** — `FileModifyDate` (all platforms) and `FileCreateDate` (Windows only) are set to match the photo/video date.

### Conditional date tag updates

Pre-existing XMP and IPTC date tags in the source file (e.g. `XMP-photoshop:DateCreated`, `IPTC:DateCreated`, `XMP-xmp:MetadataDate`) are updated to the resolved datetime only when they already exist. Tags that are absent in the source are left absent — the merger does not inject new date tags beyond the core set.

### XMP sidecar fixup

When creating XMP sidecars via ExifTool's `-o` command, ExifTool copies existing metadata from the source file before applying overrides. This means pre-existing tags — including non-XMP dates such as Nikon maker-note `CreateDate` — can be mapped into the XMP sidecar and override the values passed as parameters, often without the timezone suffix. To handle this, a second in-place fixup pass always force-writes the three core sidecar date tags (`XMP-xmp:CreateDate`, `XMP-xmp:ModifyDate`, `XMP-exif:DateTimeOriginal`) with the correct datetime and timezone, plus any additional conditional date tags that existed in the source.

### Extension mismatch detection

The merger detects content/extension mismatches by comparing ExifTool's `FileTypeExtension` against the source file's extension. Equivalent extensions (`.jpg`/`.jpeg`, `.tif`/`.tiff`, `.mov`/`.mp4`/`.m4v`/`.qt`) are not flagged. True mismatches (e.g. JPEG content with a `.dng` extension) trigger a temporary rename during processing so ExifTool writes tags correctly, then the original extension is restored.

### Blocking unwanted descriptions

Edit the `blocked_descriptions` list in the `__main__` block of `GooglePhotosExportMerger.py` to skip descriptions you don't want written into EXIF. For example:

```python
blocked_descriptions = [
    "SONY DSC",
    "OLYMPUS DIGITAL CAMERA",
]
```

When a blocked description is detected, the merger clears the description from `EXIF:UserComment`, `EXIF:ImageDescription`, and `XMP-dc:Description`. If the source file also has `IPTC:Caption-Abstract`, that is cleared too.

---

## Testing

Run the test suite with:

```bash
python -m pytest TestMerger.py
```

`TestMerger.py` is a comprehensive `unittest`-based integration test suite with 185+ test methods (plus subtests). `setUpClass` builds an input tree with programmatically generated binary test files for all supported formats (JPEG, PNG, GIF, TIFF, CR2, DNG, HEIC, MP4, MOV, AVI, MKV, WebM — including variants with embedded EXIF timezone offsets and Nikon maker-note dates), runs the merger once, then individual tests assert on the output.

Test categories: input integrity, output structure, GPS (8 compass directions × 12 formats), timezones (including sidecar timezone verification with various offsets), descriptions (UTF-8, escaping, newlines, blocked, IPTC), file types, orphan files, XMP conditional dates, XMP sidecars, duplicates, bracket notation, file timestamps, stats verification, video UTC time, special filenames, EXIF preservation, extension mismatch, video XMP dates (including Nikon maker-note sidecar fixup), infrastructure validation, and single-worker (serial) mode.

A `TestSingleWorker` class re-runs the merger with `num_workers=1` and verifies stats and output match the parallel run.

---

## Architecture

Five modules:

1. **AbstractMediaMerger.py** — Abstract base class defining the 9-step merge pipeline, dataclasses (`WriteStrategy`, `MediaFileInfo`, `MergeStats`), GPS resolution, duplicate resolution, dry-run logging, and summary reporting.
2. **GooglePhotosExportMerger.py** — Concrete implementation and CLI entry point. Builds ExifTool parameters, implements parallel processing via `ProcessPoolExecutor`.
3. **JsonFileIdentifier.py** — Matches JSON metadata files to media files using `SortedSet` prefix lookups. Handles bracket notation and case-insensitive extensions.
4. **JsonKeyExtractor.py** — Analysis tool. Scans directory trees and generates structural reports.
5. **TestMerger.py** — Integration test suite.

**Data flow:** JsonKeyExtractor scans directories → JsonFileIdentifier matches JSON-to-media files → GooglePhotosExportMerger writes metadata to EXIF.