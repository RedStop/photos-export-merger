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
- Only tested on Windows.

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
| **Video (XMP sidecar only)** | `.avi`, `.mkv`, `.mov`, `.mp4`, `.m4v`, `.webm` |

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

1. Matches each JSON metadata file to its corresponding media file.
2. Writes metadata (dates, descriptions, GPS coordinates) into the media file's EXIF tags using ExifTool. Descriptions that contain only whitespace are automatically skipped.
3. Updates the file creation time and file modified time to match the photo/video date.
4. Copies the updated media file into the output directory, organized into `YYYY/MM/filename` subdirectories based on the photo/video date.
5. **Orphan files** (media with no matching JSON) are still copied to the output — they just won't have updated metadata. Their dates are resolved from existing EXIF tags or the filesystem creation date.

Steps 1–3 (scanning, matching, date resolution) run serially. Step 4 (file processing) runs in parallel across multiple worker processes when `--workers` is greater than 1, with each worker managing its own ExifTool instance. Both serial and parallel paths share the same core processing logic — the class methods delegate to shared module-level functions that are also used by the parallel workers.

### Blocking unwanted descriptions

Edit the `blocked_descriptions` list in the `__main__` block of `GooglePhotosExportMerger.py` to skip descriptions you don't want written into EXIF. For example:

```python
blocked_descriptions = [
    "SONY DSC",
    "OLYMPUS DIGITAL CAMERA",
]
```