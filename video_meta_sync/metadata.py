"""
Metadata extraction from original video files using ExifTool.

Responsibilities
----------------
* Run exiftool on the original video and return a flat tag dict.
* Parse all date/time fields, normalise to UTC, and select the *earliest*
  as the "original time taken".
* Determine the "recording timezone": the UTC offset of the winning field
  (or the local machine timezone if no offset is available).
* Expose a clean VideoMetadata dataclass consumed by other modules.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, tzinfo
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import exiftool

import re

from .constants import ALL_DATETIME_TAGS, DATETIME_TAGS_UTC, GPS_TAGS

log = logging.getLogger(__name__)

# exiftool datetime format without timezone
_EXIF_FMT = "%Y:%m:%d %H:%M:%S"
# exiftool datetime format with timezone offset (±HH:MM)
_EXIF_FMT_TZ = "%Y:%m:%d %H:%M:%S%z"

# Matches any QuickTime track-level date atom, e.g.:
#   QuickTime:Track3:TrackCreateDate
#   QuickTime:Track4:MediaCreateDate
#   QuickTime:Track10:TrackModifyDate
_TRACK_DATE_RE = re.compile(
    r"^QuickTime:Track\d+:(?:Track|Media)(?:Create|Modify)Date$"
)


def _collect_datetime_tags(raw_tags: dict[str, Any]) -> dict[str, tuple[str, bool]]:
    """
    Return every date/time tag present in *raw_tags* together with a flag
    indicating whether the value must be treated as UTC.

    Static tags defined in ALL_DATETIME_TAGS are included first.
    Then every tag matching _TRACK_DATE_RE that was not already listed is
    appended — all QuickTime track-level atoms are UTC by spec.

    Returns
    -------
    dict mapping tag_name → (raw_value_str, is_utc_only)
    """
    result: dict[str, tuple[str, bool]] = {}

    # Static tags (order matters: TZ-aware first, then UTC-only).
    for tag in ALL_DATETIME_TAGS:
        if tag in raw_tags:
            result[tag] = (str(raw_tags[tag]), tag in DATETIME_TAGS_UTC)

    # Dynamic track-level tags not already covered.
    for key, value in raw_tags.items():
        if key not in result and _TRACK_DATE_RE.match(key):
            result[key] = (str(value), True)  # always UTC by QuickTime spec

    return result


def _local_tz() -> tzinfo:
    """Return the local machine's timezone."""
    return datetime.now(timezone.utc).astimezone().tzinfo  # type: ignore[return-value]


def _parse_exif_datetime(value: str) -> datetime | None:
    """
    Parse an exiftool datetime string into an aware or naïve datetime.

    Handles:
      "2021:04:10 18:15:34"           → naïve
      "2021:04:10 20:15:34+02:00"     → aware
      "2021:04:10 20:15:34Z"          → aware UTC
    """
    if not value or value.startswith("0000"):
        return None

    value = value.strip()

    # Normalise "Z" suffix → "+00:00" so strptime %z can handle it.
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    # Try aware parse first.
    try:
        return datetime.strptime(value, _EXIF_FMT_TZ)
    except ValueError:
        pass

    # Fall back to naïve parse.
    try:
        return datetime.strptime(value, _EXIF_FMT)
    except ValueError:
        log.debug("Could not parse datetime value: %r", value)
        return None


def _to_utc(dt: datetime) -> datetime:
    """Convert an aware or naïve datetime to UTC. Naïve → assumed UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass
class VideoMetadata:
    """
    All metadata extracted from the original video plus derived values.

    Attributes
    ----------
    raw_tags:
        The complete flat tag dictionary returned by exiftool.
    original_time_taken:
        The earliest datetime found across all time fields, normalised to UTC.
    recording_tz:
        The timezone in which the video was recorded.  If the winning field
        carried offset information this is used; otherwise the local machine
        timezone is used.
    gps_tags:
        Subset of raw_tags containing only GPS-related tags (may be empty).
    """

    raw_tags: dict[str, Any]
    original_time_taken: datetime          # always UTC-aware
    recording_tz: tzinfo
    gps_tags: dict[str, Any] = field(default_factory=dict)

    @property
    def original_time_in_recording_tz(self) -> datetime:
        """original_time_taken expressed in recording_tz."""
        return self.original_time_taken.astimezone(self.recording_tz)

    @property
    def original_time_utc_str(self) -> str:
        """UTC datetime formatted for exiftool (no offset suffix)."""
        return self.original_time_taken.strftime(_EXIF_FMT)

    @property
    def original_time_local_str(self) -> str:
        """
        Datetime in recording_tz formatted for exiftool WITH offset suffix,
        e.g. "2021:04:10 20:15:34+02:00".
        """
        local_dt = self.original_time_in_recording_tz
        # Format offset as ±HH:MM
        offset = local_dt.strftime("%z")          # e.g. "+0200"
        if len(offset) == 5:
            offset = offset[:3] + ":" + offset[3:]  # → "+02:00"
        return local_dt.strftime(_EXIF_FMT) + offset


def _select_earliest_time(
    raw_tags: dict[str, Any],
) -> tuple[datetime, tzinfo] | None:
    """
    Iterate all date/time tags (static + dynamically discovered track-level
    atoms), parse each, convert to UTC, and return (earliest_utc, recording_tz).

    The recording timezone is taken from the winning field if it was
    timezone-aware; otherwise the local machine timezone is used.
    """
    best_utc: datetime | None = None
    best_tz: tzinfo | None = None

    for tag, (raw_value, is_utc_only) in _collect_datetime_tags(raw_tags).items():
        dt = _parse_exif_datetime(raw_value)
        if dt is None:
            continue

        # Tags in DATETIME_TAGS_UTC have no offset in the file; treat as UTC.
        if is_utc_only and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        utc_dt = _to_utc(dt)

        if best_utc is None or utc_dt < best_utc:
            best_utc = utc_dt
            # Preserve the original offset if the field had one.
            if dt.tzinfo is not None and not is_utc_only:
                best_tz = dt.tzinfo
            else:
                best_tz = None  # will fall back to local machine tz later

    if best_utc is None:
        return None

    recording_tz: tzinfo = best_tz if best_tz is not None else _local_tz()
    return best_utc, recording_tz


def extract_metadata(original: Path) -> VideoMetadata | None:
    """
    Run exiftool on *original* and return a populated VideoMetadata, or None
    if extraction fails or no usable datetime is found.
    """
    try:
        with exiftool.ExifToolHelper() as et:
            # -G0:1 returns tags like "QuickTime:Keys:CreationDate"
            results = et.get_tags(
                str(original),
                tags=None,  # extract everything
                params=["-G0:1"],
            )
    except exiftool.exceptions.ExifToolExecuteError as exc:
        log.error("exiftool failed for '%s': %s", original, exc)
        return None

    if not results:
        log.error("exiftool returned no data for '%s'.", original)
        return None

    raw_tags: dict[str, Any] = results[0]

    time_result = _select_earliest_time(raw_tags)
    if time_result is None:
        log.error("No usable datetime found in '%s'.", original)
        return None

    original_time_taken, recording_tz = time_result

    gps_tags = {k: v for k, v in raw_tags.items() if k in GPS_TAGS}

    log.debug(
        "'%s' → original time taken: %s (recording tz: %s)",
        original,
        original_time_taken.isoformat(),
        recording_tz,
    )

    return VideoMetadata(
        raw_tags=raw_tags,
        original_time_taken=original_time_taken,
        recording_tz=recording_tz,
        gps_tags=gps_tags,
    )
