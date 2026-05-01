"""
Constants used across the video_meta_sync package.
"""

# All recognised video container extensions (lowercase, without leading dot).
# Used to identify candidate "original" video files when scanning for pairs.
KNOWN_VIDEO_EXTENSIONS: frozenset[str] = frozenset(
    {
        "mp4",
        "mov",
        "mkv",
        "avi",
        "wmv",
        "flv",
        "webm",
        "m4v",
        "mpg",
        "mpeg",
        "3gp",
        "3g2",
        "ts",
        "mts",
        "m2ts",
        "vob",
        "ogv",
        "rm",
        "rmvb",
        "divx",
        "xvid",
        "f4v",
        "mxf",
        "dv",
        "asf",
    }
)

# Formats whose containers support writing metadata back via exiftool.
# All other output formats (e.g. MKV) get a sidecar only.
METADATA_WRITABLE_EXTENSIONS: frozenset[str] = frozenset({"mp4", "mov", "m4v"})

# ---------------------------------------------------------------------------
# ExifTool tag names used for date/time extraction.
# ---------------------------------------------------------------------------

# Tags that *may* carry timezone offset information.
DATETIME_TAGS_WITH_TZ: tuple[str, ...] = (
    "QuickTime:Keys:CreationDate",  # Apple – has offset e.g. 2021:04:10 20:15:34+02:00
    "File:System:FileCreateDate",   # exiftool synthesised – has local offset
    "File:System:FileModifyDate",
)

# Tags that are stored as plain UTC (no offset in the value itself).
# Track-level TrackCreateDate / MediaCreateDate tags are intentionally
# absent here — they are discovered dynamically from the raw ExifTool
# output at runtime (see metadata._collect_datetime_tags), because track
# numbering varies by container and muxer and cannot be predicted in advance.
DATETIME_TAGS_UTC: tuple[str, ...] = (
    "QuickTime:CreateDate",
)

# All datetime tags in one tuple (TZ-aware first).
ALL_DATETIME_TAGS: tuple[str, ...] = DATETIME_TAGS_WITH_TZ + DATETIME_TAGS_UTC

# ---------------------------------------------------------------------------
# GPS tags
# ---------------------------------------------------------------------------
GPS_TAGS: tuple[str, ...] = (
    "Composite:GPSLatitude",
    "Composite:GPSLongitude",
    "Composite:GPSAltitude",
    "Composite:GPSAltitudeRef",
    "Composite:GPSPosition",
    "QuickTime:Keys:GPSCoordinates",
    "QuickTime:UserData:GPSCoordinates",
)

# Date/time tag *base names* (no group prefix) written back into writable
# containers and used as keys inside the XMP sidecar.
EXIFTOOL_DATE_WRITE_TAGS: tuple[str, ...] = (
    "CreateDate",
    "ModifyDate",
    "TrackCreateDate",
    "TrackModifyDate",
    "MediaCreateDate",
    "MediaModifyDate",
)
