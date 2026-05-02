"""
XMP sidecar writer — implemented via ExifTool.

ExifTool writes well-formed, schema-correct XMP sidecars natively, handling
all namespace declarations, tag mappings, and GPS encoding automatically.
This replaces the previous manual XML construction.

Strategy (single ExifTool call per pair)
-----------------------------------------
1. ``-o %d%f.%e.xmp``
       Directs output to a sidecar named "<dir>/<stem>.<ext>.xmp", e.g.
       "video.mkv.xmp".  The format string tokens are ExifTool's own:
         %d – directory,  %f – filename stem,  %e – extension.

2. ``-tagsfromfile <original>``
       Copies every tag from the original video into the sidecar.

3. Explicit ``-TAG=VALUE`` overrides
       All date/time fields are then replaced with the normalised
       "original time taken" value so the sidecar timestamps are
       consistent and correct regardless of what was in the original.
"""

from __future__ import annotations

import logging
from pathlib import Path

import exiftool

from .constants import EXIFTOOL_DATE_WRITE_TAGS
from .metadata import VideoMetadata

log = logging.getLogger(__name__)


def _build_gps_args(meta: VideoMetadata) -> list[str]:
    """
    Return exiftool argument strings that write decimal GPS values into the
    sidecar using XMP tags.

    ``-tagsfromfile`` cannot copy ``Composite:*`` tags (they are synthesised
    by exiftool at read time and have no underlying atom to copy), so GPS must
    always be written explicitly from the pre-extracted decimal values stored
    in ``meta.gps_decimal``.
    """
    args: list[str] = []
    lat = meta.gps_decimal.get("GPSLatitude")
    lon = meta.gps_decimal.get("GPSLongitude")
    alt = meta.gps_decimal.get("GPSAltitude")

    if lat is not None:
        args.append(f"-XMP:GPSLatitude={lat}")
    if lon is not None:
        args.append(f"-XMP:GPSLongitude={lon}")
    if alt is not None:
        args.append(f"-XMP:GPSAltitude={alt}")

    return args


def _build_date_override_args(meta: VideoMetadata) -> list[str]:
    """
    Return exiftool argument strings that override every date/time field
    with the normalised "original time taken".

    QuickTime container date atoms have no timezone storage, so they receive
    the plain UTC value.  The Apple-specific Keys:CreationDate atom and the
    XMP date fields do support an offset, so they receive the recording-tz
    value (e.g. "2021:04:10 20:15:34+02:00").
    """
    utc_str   = meta.original_time_utc_str    # "2021:04:10 18:15:34"
    local_str = meta.original_time_local_str  # "2021:04:10 20:15:34+02:00"

    args: list[str] = []

    # QuickTime atoms (no timezone)
    for tag in EXIFTOOL_DATE_WRITE_TAGS:
        args.append(f"-{tag}={utc_str}")

    # Apple CreationDate key (supports offset)
    args.append(f"-Keys:CreationDate={local_str}")

    # XMP date fields (support offset)
    for xmp_tag in ("XMP:CreateDate", "XMP:ModifyDate", "XMP:MetadataDate"):
        args.append(f"-{xmp_tag}={local_str}")

    return args


def write_sidecar(
    reencoded: Path,
    meta: VideoMetadata,
    original: Path,
    dry_run: bool = False,
) -> Path:
    """
    Write an XMP sidecar for *reencoded* by copying all tags from *original*
    and overriding every date/time field with the normalised value from *meta*.

    The sidecar is named "<reencoded_filename>.xmp", e.g. "video.mkv.xmp".
    Returns the sidecar path.
    """
    sidecar_path = reencoded.parent / (reencoded.name + ".xmp")

    if dry_run:
        log.info("[DRY-RUN] Would write sidecar '%s'.", sidecar_path)
        return sidecar_path

    date_args = _build_date_override_args(meta)
    gps_args  = _build_gps_args(meta)

    try:
        with exiftool.ExifToolHelper() as et:
            et.execute(
                # Output to sidecar with double-extension name.
                "-o", "%d%f.%e.xmp",
                # Copy all tags from the original.
                "-tagsfromfile", str(original),
                "-all:all",
                # Override all date/time fields.
                *date_args,
                # Write decimal GPS (Composite tags are not copied by
                # -tagsfromfile, so these must be set explicitly).
                *gps_args,
                # Source file that provides the output filename template.
                str(reencoded),
            )
        log.info("Wrote sidecar '%s'.", sidecar_path)
    except exiftool.exceptions.ExifToolExecuteError as exc:
        log.error("Failed to write sidecar '%s': %s", sidecar_path, exc)

    return sidecar_path


def sidecar_exists(reencoded: Path) -> bool:
    """Return True if the XMP sidecar for *reencoded* already exists."""
    return (reencoded.parent / (reencoded.name + ".xmp")).exists()
