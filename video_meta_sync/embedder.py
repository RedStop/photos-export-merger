"""
Embed metadata into re-encoded video files that support it (MP4, MOV, M4V).

For MKV and other containers that have limited exiftool write support, this
module does nothing and the caller falls back to the XMP sidecar only.

What is written
---------------
* All date/time fields → "original time taken"
  - Tags without timezone support receive the UTC value.
  - Tags that support a timezone offset receive the recording-tz value.
* GPS coordinates (if present in the original).
* Make, Model, Software (if present in the original).
"""

from __future__ import annotations

import logging
from pathlib import Path

import exiftool

from .constants import (
    EXIFTOOL_DATE_WRITE_TAGS,
    METADATA_WRITABLE_EXTENSIONS,
    GPS_TAGS,
)
from .metadata import VideoMetadata

log = logging.getLogger(__name__)


def _is_writable_container(path: Path) -> bool:
    ext = path.suffix.lstrip(".").lower()
    return ext in METADATA_WRITABLE_EXTENSIONS


def _build_tag_args(meta: VideoMetadata) -> list[str]:
    """
    Build the list of exiftool -TAG=VALUE arguments for the re-encoded file.
    """
    args: list[str] = []

    # --- Date/time fields ---
    # QuickTime date tags do not store a timezone offset in the atom itself;
    # the Apple-specific "CreationDate" key does support an offset.
    utc_str   = meta.original_time_utc_str    # "2021:04:10 18:15:34"
    local_str = meta.original_time_local_str  # "2021:04:10 20:15:34+02:00"

    for tag in EXIFTOOL_DATE_WRITE_TAGS:
        args.append(f"-{tag}={utc_str}")

    # Apple CreationDate key (supports offset).
    args.append(f"-Keys:CreationDate={local_str}")

    # --- GPS ---
    lat = meta.gps_tags.get("Composite:GPSLatitude")
    lon = meta.gps_tags.get("Composite:GPSLongitude")
    alt = meta.gps_tags.get("Composite:GPSAltitude")

    if lat is not None:
        args.append(f"-GPSLatitude={lat}")
    if lon is not None:
        args.append(f"-GPSLongitude={lon}")
    if alt is not None:
        args.append(f"-GPSAltitude={alt}")
        alt_ref = meta.gps_tags.get("Composite:GPSAltitudeRef", "Above Sea Level")
        ref_val = "0" if "above" in str(alt_ref).lower() else "1"
        args.append(f"-GPSAltitudeRef={ref_val}")

    # --- Device info ---
    for exif_key, write_tag in [
        ("QuickTime:Keys:Make",     "Make"),
        ("QuickTime:Keys:Model",    "Model"),
        ("QuickTime:Keys:Software", "Software"),
    ]:
        value = meta.raw_tags.get(exif_key)
        if value is not None:
            # Escape any = in the value
            escaped = str(value).replace("=", r"\=")
            args.append(f"-{write_tag}={escaped}")

    return args


def embed_metadata(
    reencoded: Path,
    meta: VideoMetadata,
    dry_run: bool = False,
) -> bool:
    """
    Embed metadata from *meta* into *reencoded* if the container supports it.

    Returns True if embedding was attempted (or would be in dry-run),
    False if the container is not writable (caller should use sidecar only).
    """
    if not _is_writable_container(reencoded):
        log.debug(
            "'%s' is not a writable container; skipping metadata embedding.",
            reencoded,
        )
        return False

    tag_args = _build_tag_args(meta)

    if dry_run:
        log.info(
            "[DRY-RUN] Would embed %d metadata tag(s) into '%s'.",
            len(tag_args),
            reencoded,
        )
        return True

    try:
        with exiftool.ExifToolHelper() as et:
            et.execute(
                *tag_args,
                "-overwrite_original",
                str(reencoded),
            )
        log.info("Embedded metadata into '%s'.", reencoded)
    except exiftool.exceptions.ExifToolExecuteError as exc:
        log.error("Failed to embed metadata into '%s': %s", reencoded, exc)

    return True
