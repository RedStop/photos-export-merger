#!/usr/bin/env python3
"""
Process reencoded video files and update associated JSON metadata.

For each original video file (.mp4, .mov, etc.), this script:
  - Checks for a corresponding .mkv reencoded file
  - Checks for a corresponding .json metadata file
  - Renames the .json file to match the .mkv filename
  - Updates the title field extension in the .json from the original to .mkv
  - Optionally deletes the original video file
"""

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path


# ── Configuration ─────────────────────────────────────────────────────────────

# Add more extensions here as needed
ORIGINAL_VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mpeg", ".m4v"}

REENCODED_EXTENSION = ".mkv"

LOG_FILE = "process_reencoded_videos.log"


# ── Logging setup ──────────────────────────────────────────────────────────────

def setup_logging(log_file: str) -> None:
    log_format = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(
        level=logging.DEBUG,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


# ── Directory scanning ─────────────────────────────────────────────────────────

def scan_directory(root: Path) -> dict[str, Path]:
    """
    Walk the directory tree once and return a dict mapping
    each filename (case-sensitive) to its full Path.
    """
    file_map: dict[str, Path] = {}
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in filenames:
            full_path = Path(dirpath) / filename
            # In case of duplicates across subdirectories, keep the first found.
            # Use relative path as key to support same filenames in different dirs.
            rel = str(full_path.relative_to(root))
            file_map[rel] = full_path
    return file_map


def build_lookup(file_map: dict[str, Path]) -> dict[str, set[str]]:
    """
    Build a reverse lookup: directory (relative to root) → set of filenames.
    This lets us quickly check whether a file exists in the same folder.
    """
    lookup: dict[str, set[str]] = {}
    for rel, path in file_map.items():
        folder = str(path.parent)
        lookup.setdefault(folder, set()).add(path.name)
    return lookup


# ── Core processing ────────────────────────────────────────────────────────────

def update_json_title_extension(json_path: Path, old_ext: str, new_ext: str) -> bool:
    """
    Replace the extension in the 'title' field value from old_ext to new_ext
    using a targeted text substitution so that all other formatting is preserved.

    Returns True on success, False on failure.
    """
    try:
        text = json_path.read_text(encoding="utf-8")
    except OSError as exc:
        logging.error("Cannot read %s: %s", json_path, exc)
        return False

    # Match the title key and its value so we only replace the extension
    # inside the title value, not anywhere else in the file.
    # Pattern: "title": "....<old_ext>"  (value ends with old_ext before closing quote)
    escaped_old = re.escape(old_ext)
    pattern = r'("title"\s*:\s*"(?:[^"\\]|\\.)*?)' + escaped_old + r'(")'
    replacement = r'\g<1>' + new_ext + r'\g<2>'

    new_text, count = re.subn(pattern, replacement, text, count=1)
    if count == 0:
        logging.error(
            "Could not find title field ending with %s in %s", old_ext, json_path
        )
        return False

    try:
        json_path.write_text(new_text, encoding="utf-8")
    except OSError as exc:
        logging.error("Cannot write %s: %s", json_path, exc)
        return False

    return True


def get_title_extension(json_path: Path) -> str | None:
    """
    Parse the JSON file and return the file extension found in the 'title' field,
    or None if the field is missing or cannot be parsed.
    """
    try:
        text = json_path.read_text(encoding="utf-8")
        data = json.loads(text)
    except (OSError, json.JSONDecodeError) as exc:
        logging.error("Cannot parse %s: %s", json_path, exc)
        return None

    title = data.get("title", "")
    if not title:
        logging.error("'title' field is empty or missing in %s", json_path)
        return None

    return Path(title).suffix  # e.g. ".mp4"


def process_video(
    video_path: Path,
    folder_files: set[str],
    delete_original: bool,
) -> None:
    """Process a single original video file."""
    video_name = video_path.name          # e.g. "clip.mp4"
    video_stem = video_path.stem          # e.g. "clip"
    video_ext  = video_path.suffix        # e.g. ".mp4"
    folder     = video_path.parent

    logging.info("Processing: %s", video_path)

    # 1. Check for reencoded .mkv file
    mkv_name = video_stem + REENCODED_EXTENSION   # e.g. "clip.mkv"
    if mkv_name not in folder_files:
        logging.warning("SKIP — no reencoded %s found for: %s", REENCODED_EXTENSION, video_path)
        return

    # 2. Check for .json metadata file
    json_old_name = video_name + ".json"          # e.g. "clip.mp4.json"
    if json_old_name not in folder_files:
        logging.warning("SKIP — no .json metadata file found for: %s", video_path)
        return

    json_old_path = folder / json_old_name

    # 3. Determine new .json filename  e.g. "clip.mkv.json"
    json_new_name = mkv_name + ".json"
    json_new_path = folder / json_new_name

    # 4. Check that the new .json name doesn't already exist
    if json_new_name in folder_files:
        logging.warning("SKIP — target JSON already exists: %s", json_new_path)
        return

    # 5. Validate that the title extension matches the original video extension
    title_ext = get_title_extension(json_old_path)
    if title_ext is None:
        return  # error already logged inside get_title_extension

    if title_ext.lower() != video_ext.lower():
        logging.warning(
            "SKIP — title extension (%s) does not match video extension (%s) in: %s",
            title_ext, video_ext, json_old_path,
        )
        return

    # 6. Update the title extension inside the JSON (text edit, preserves formatting)
    if not update_json_title_extension(json_old_path, title_ext, REENCODED_EXTENSION):
        return  # error already logged

    # 7. Rename the .json file
    try:
        json_old_path.rename(json_new_path)
    except OSError as exc:
        logging.error("Failed to rename %s → %s: %s", json_old_path, json_new_path, exc)
        # Attempt to roll back the title change
        update_json_title_extension(json_old_path, REENCODED_EXTENSION, title_ext)
        return

    logging.info("SUCCESS — renamed %s → %s and updated title extension.", json_old_name, json_new_name)

    # 8. Optionally delete the original video
    if delete_original:
        try:
            video_path.unlink()
            logging.info("DELETED original video: %s", video_path)
        except OSError as exc:
            logging.error("Failed to delete original video %s: %s", video_path, exc)


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Update JSON metadata files after reencoding videos to MKV."
    )
    parser.add_argument(
        "directory",
        type=Path,
        help="Root directory (including sub-directories) to process.",
    )
    parser.add_argument(
        "--delete-originals",
        action="store_true",
        default=False,
        help="Delete the original video file after successful processing.",
    )
    args = parser.parse_args()

    root: Path = args.directory.resolve()
    if not root.is_dir():
        print(f"Error: '{root}' is not a directory.", file=sys.stderr)
        sys.exit(1)

    setup_logging(LOG_FILE)
    logging.info("=== Starting processing in: %s ===", root)
    logging.info("Delete originals: %s", args.delete_originals)
    logging.info(
        "Target video extensions: %s",
        ", ".join(sorted(ORIGINAL_VIDEO_EXTENSIONS)),
    )

    # Scan the entire directory tree once
    logging.info("Scanning directory tree…")
    file_map = scan_directory(root)
    folder_lookup = build_lookup(file_map)
    logging.info("Found %d files total.", len(file_map))

    # Collect all original video files
    video_paths = [
        path
        for path in file_map.values()
        if path.suffix.lower() in ORIGINAL_VIDEO_EXTENSIONS
    ]
    logging.info(
        "Found %d original video file(s) with extensions %s.",
        len(video_paths),
        ", ".join(sorted(ORIGINAL_VIDEO_EXTENSIONS)),
    )

    for video_path in sorted(video_paths):
        folder_str = str(video_path.parent)
        folder_files = folder_lookup.get(folder_str, set())
        process_video(video_path, folder_files, args.delete_originals)

    logging.info("=== Processing complete. ===")


if __name__ == "__main__":
    main()