# video_meta_sync

Synchronise metadata from **original** video files onto their **re-encoded**
counterparts.

---

## What it does

When you re-encode a video (e.g. with HandBrake or FFmpeg) the output file
often loses all embedded metadata: dates, GPS coordinates, camera make/model,
etc.  `video_meta_sync` automates restoring that metadata:

1. **Scans** a directory tree (recursively) for pairs of
   `<stem>.<output_ext>` (re-encoded) and `<stem>.<any_other_video_ext>`
   (original).
2. **Extracts** all metadata from the original using
   [ExifTool](https://exiftool.org/).
3. Selects the **"original time taken"** — the *earliest* date/time found
   across all time fields in the original (including the filesystem creation
   date), normalised to UTC for comparison.
4. **Embeds** corrected metadata back into the re-encoded file when the
   container supports it (MP4, MOV, M4V).
5. **Writes an XMP sidecar** file (`<reencoded>.mkv.xmp`) containing all
   metadata with every date field set to the original time taken.
6. **Updates the filesystem timestamps** (modification date on all platforms;
   additionally creation date on Windows) of the re-encoded file.

The tool is **idempotent**: if a sidecar already exists for a re-encoded file
all processing for that file is skipped.

---

## Requirements

| Requirement | Notes |
|---|---|
| Python ≥ 3.11 | Uses `zoneinfo`, `match`, and `X \| Y` type hints |
| [ExifTool](https://exiftool.org/) | Must be on `PATH` |
| `PyExifTool` Python package | `pip install PyExifTool` |

---

## Installation

No packaging is required.  Clone or copy the `video_meta_sync/` directory
alongside your videos (or anywhere on `PYTHONPATH`) and run it as a module:

```bash
python -m video_meta_sync [OPTIONS] [TARGET]
```

---

## Usage

```
usage: video_meta_sync [-h] [--output-ext EXT] [--no-sidecar] [--dry-run]
                       [--log-level {DEBUG,INFO,WARNING,ERROR}]
                       [target]

positional arguments:
  target                Path to a directory (scanned recursively) or to a
                        specific re-encoded video file.
                        Defaults to the current directory.

options:
  -h, --help            show this help message and exit
  --output-ext EXT      Extension (without dot) of re-encoded output videos.
                        Default: mkv
  --no-sidecar          Skip creating XMP sidecar files.
  --dry-run             Log what would be done without making any changes.
  --log-level {DEBUG,INFO,WARNING,ERROR}
                        Logging verbosity. Default: INFO
```

### Examples

```bash
# Process the current directory recursively (looks for *.mkv)
python -m video_meta_sync

# Process a specific directory
python -m video_meta_sync /Volumes/Media/Videos/2021

# Process a single re-encoded file
python -m video_meta_sync /Volumes/Media/Videos/2021/birthday.mkv

# Re-encoded files have a .mp4 extension instead of the default .mkv
python -m video_meta_sync --output-ext mp4 /path/to/videos

# Don't create XMP sidecars, just fix file dates
python -m video_meta_sync --no-sidecar /path/to/videos

# Preview what would happen without touching any files
python -m video_meta_sync --dry-run /path/to/videos

# Verbose output for debugging
python -m video_meta_sync --log-level DEBUG /path/to/videos
```

---

## Module structure

```
video_meta_sync/
├── __init__.py       – package marker and version
├── __main__.py       – entry point; wires CLI → scanner → processor
├── cli.py            – argparse definitions
├── constants.py      – known extensions, tag name lists
├── scanner.py        – find (re-encoded, original) pairs
├── metadata.py       – exiftool extraction; "original time taken" logic
├── embedder.py       – write metadata into MP4/MOV containers
├── xmp.py            – build and write XMP sidecar files
├── file_dates.py     – update filesystem timestamps (cross-platform)
├── processor.py      – orchestrate steps for one pair
└── requirements.txt
```

---

## Date/time selection logic

The following fields are read from the **original** video and all values are
converted to UTC for comparison:

| Field | Timezone assumption |
|---|---|
| `QuickTime:Keys:CreationDate` | Carries its own offset (e.g. `+02:00`) |
| `File:System:FileCreateDate` | Carries its own offset |
| `File:System:FileModifyDate` | Carries its own offset |
| `QuickTime:CreateDate` | Assumed UTC (QuickTime spec) |
| `QuickTime:Track*:TrackCreateDate` | Assumed UTC |
| `QuickTime:Track*:MediaCreateDate` | Assumed UTC |

The **earliest** UTC value is chosen as the "original time taken".

**Recording timezone**: taken from the winning field's offset if it had one;
otherwise the local machine's timezone (`datetime.now(timezone.utc).astimezone().tzinfo`) is used.

All date/time values written back (to the file, to the sidecar, and to the
filesystem) use this recording timezone.

---

## GPS handling

If the original file contains GPS coordinates (`GPSLatitude`, `GPSLongitude`,
`GPSAltitude`), they are:

* Written into the re-encoded file's container (MP4/MOV only).
* Written into the XMP sidecar (`exif:GPSLatitude`, etc.).

---

## Idempotency

Before processing any pair the script checks whether
`<reencoded_filename>.xmp` already exists.  If it does, **all** steps
(metadata embedding, sidecar creation, file-date update) are skipped and the
pair is logged as "skipped".

---

## Platform notes

| Platform | Creation date | Modification date |
|---|---|---|
| Windows | ✅ Set | ✅ Set |
| Linux / macOS | ❌ Not settable via standard APIs | ✅ Set |

---

## Supported video container extensions

The scanner recognises the following extensions as potential "original" video
files:

`mp4` `mov` `mkv` `avi` `wmv` `flv` `webm` `m4v` `mpg` `mpeg` `3gp` `3g2`
`ts` `mts` `m2ts` `vob` `ogv` `rm` `rmvb` `divx` `xvid` `f4v` `mxf` `dv`
`asf`

---

## Error handling

| Situation | Behaviour |
|---|---|
| No original found for a re-encoded file | Logged at DEBUG; skipped |
| Multiple originals found | Logged at ERROR; skipped |
| ExifTool extraction fails | Logged at ERROR; pair skipped |
| No usable datetime in original | Logged at ERROR; pair skipped |
| Sidecar already exists | Logged at INFO; all steps skipped |
| Metadata embedding fails | Logged at ERROR; sidecar/dates still attempted |
| File date update fails | Logged at ERROR |
