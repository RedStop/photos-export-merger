"""
File date updater — implemented via ExifTool.

ExifTool supports writing FileModifyDate and FileCreateDate directly,
including timezone-aware values, and works identically on Windows, Linux,
and macOS.  This replaces the previous OS-specific implementation that
required pywin32 on Windows and could only set mtime on Linux.

Platform behaviour
------------------
Windows  : ExifTool sets both FileCreateDate and FileModifyDate.
Linux    : ExifTool sets FileModifyDate (FileCreateDate is a no-op on
           most Linux filesystems and ExifTool silently skips it).
macOS    : ExifTool sets both dates via the HFS+ / APFS creation-time API.

The timestamp written is ``meta.original_time_in_recording_tz``, i.e. the
"original time taken" expressed in the timezone the video was recorded in,
complete with UTC offset (e.g. "2021:04:10 20:15:34+02:00").
"""

from __future__ import annotations

import logging
from pathlib import Path

import exiftool

from .metadata import VideoMetadata

log = logging.getLogger(__name__)


def update_file_dates(
    reencoded: Path,
    meta: VideoMetadata,
    dry_run: bool = False,
) -> None:
    """
    Update the filesystem timestamps of *reencoded* using ExifTool.

    The value written is ``meta.original_time_local_str`` which carries a
    UTC offset, e.g. "2021:04:10 20:15:34+02:00".  ExifTool converts this
    to the correct UTC epoch timestamp when touching the file.
    """
    date_str = meta.original_time_local_str

    if dry_run:
        log.info(
            "[DRY-RUN] Would set FileModifyDate/FileCreateDate of '%s' to %s.",
            reencoded,
            date_str,
        )
        return

    try:
        with exiftool.ExifToolHelper() as et:
            et.execute(
                f"-FileModifyDate={date_str}",
                f"-FileCreateDate={date_str}",
                str(reencoded),
            )
        log.info(
            "Set file dates of '%s' to %s.", reencoded, date_str
        )
    except exiftool.exceptions.ExifToolExecuteError as exc:
        log.error("Failed to set file dates of '%s': %s", reencoded, exc)
