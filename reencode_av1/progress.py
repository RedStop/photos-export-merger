"""Per-folder progress tracking.

Each processed folder gets a small JSON file recording the outcome of every
video that has been processed or skipped, plus the final CRF for encoded
videos. On a later run, videos already listed are skipped without any probing
or logging, so no work is repeated.

The progress file is always read from and written to the real folder — it is
never staged via ``--scratch-dir`` — so it stays in sync with the source tree.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

log = logging.getLogger("reencode_av1")

PROGRESS_FILENAME = "reencode-av1-progress.json"


def progress_path_for(folder: Path) -> Path:
    """Return the progress file path for a folder."""
    return folder / PROGRESS_FILENAME


def load_progress(folder: Path) -> dict[str, dict]:
    """Load a folder's progress record as a mapping of filename -> entry dict.

    Returns an empty dict if the file is absent, unreadable, or malformed.
    """
    path = progress_path_for(folder)
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        log.warning("Could not read progress file %s: %s", path, e)
        return {}
    if not isinstance(data, dict):
        log.warning("Progress file %s is not a JSON object, ignoring", path)
        return {}
    return data


def record_progress(
    folder: Path, filename: str, status: str, crf: int | None = None
) -> None:
    """Record one video's outcome in its folder's progress file.

    Re-reads the file before writing so the on-disk record stays in sync and
    existing entries are preserved.
    """
    data = load_progress(folder)
    entry: dict = {"status": status}
    if crf is not None:
        entry["crf"] = crf
    data[filename] = entry
    path = progress_path_for(folder)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
    except OSError as e:
        log.warning("Could not write progress file %s: %s", path, e)
