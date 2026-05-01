"""
Scanner: locate (re-encoded video, original video) pairs in a directory tree.

Rules
-----
* A "re-encoded" video is any file whose extension matches *output_ext*.
* Its "original" is a file in the *same directory* that shares the same stem
  but has a *different* extension drawn from KNOWN_VIDEO_EXTENSIONS.
* If zero originals are found  → skip (not re-encoded or original is missing).
* If two or more originals are found → log an error and skip.
* If exactly one original is found → yield the pair.
"""

import logging
from dataclasses import dataclass
from pathlib import Path

from .constants import KNOWN_VIDEO_EXTENSIONS

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class VideoPair:
    """A matched (re-encoded, original) video pair."""

    reencoded: Path
    original: Path


def _find_original(reencoded: Path, output_ext: str) -> Path | None:
    """
    Given a re-encoded video path, find the single original video file in the
    same directory.

    Returns the original Path, or None if the pair should be skipped.
    """
    stem = reencoded.stem
    parent = reencoded.parent

    candidates: list[Path] = [
        p
        for p in parent.iterdir()
        if p.is_file()
        and p.stem == stem
        and p.suffix.lstrip(".").lower() in KNOWN_VIDEO_EXTENSIONS
        and p.suffix.lstrip(".").lower() != output_ext.lower()
        and p != reencoded
    ]

    if not candidates:
        log.debug(
            "No original found for re-encoded video '%s' – skipping.", reencoded
        )
        return None

    if len(candidates) > 1:
        names = ", ".join(str(c) for c in candidates)
        log.error(
            "Multiple originals found for '%s': [%s] – skipping.",
            reencoded,
            names,
        )
        return None

    return candidates[0]


def scan_directory(root: Path, output_ext: str) -> list[VideoPair]:
    """
    Recursively scan *root* for re-encoded videos and return matched pairs.
    """
    ext = output_ext.lower().lstrip(".")
    pairs: list[VideoPair] = []

    for reencoded in sorted(root.rglob(f"*.{ext}")):
        if not reencoded.is_file():
            continue
        original = _find_original(reencoded, ext)
        if original is not None:
            pairs.append(VideoPair(reencoded=reencoded, original=original))

    log.info("Found %d re-encoded/original pair(s) under '%s'.", len(pairs), root)
    return pairs


def scan_single_file(reencoded: Path, output_ext: str) -> VideoPair | None:
    """
    Treat *reencoded* as a single re-encoded video and locate its original.
    """
    ext = output_ext.lower().lstrip(".")

    if reencoded.suffix.lstrip(".").lower() != ext:
        log.warning(
            "'%s' does not have the expected re-encoded extension '.%s'.",
            reencoded,
            ext,
        )

    original = _find_original(reencoded, ext)
    if original is None:
        return None
    return VideoPair(reencoded=reencoded, original=original)
