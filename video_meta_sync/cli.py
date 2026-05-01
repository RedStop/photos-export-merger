"""
Command-line interface definition for video_meta_sync.
"""

import argparse
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video_meta_sync",
        description=(
            "Detect re-encoded videos, extract metadata from their originals, "
            "write XMP sidecar files, update file dates, and (for writable "
            "containers) embed metadata back into the re-encoded file."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process the current directory recursively
  python -m video_meta_sync

  # Process a specific directory
  python -m video_meta_sync /path/to/videos

  # Process a single re-encoded file
  python -m video_meta_sync /path/to/video.mkv

  # Use .mp4 as the output extension instead of the default .mkv
  python -m video_meta_sync --output-ext mp4 /path/to/videos

  # Skip creating XMP sidecar files
  python -m video_meta_sync --no-sidecar /path/to/videos

  # Dry-run: log what would happen without making changes
  python -m video_meta_sync --dry-run /path/to/videos
""",
    )

    parser.add_argument(
        "target",
        nargs="?",
        type=Path,
        default=None,
        help=(
            "Path to a directory (scanned recursively) or to a specific "
            "re-encoded video file. Defaults to the current directory."
        ),
    )

    parser.add_argument(
        "--output-ext",
        metavar="EXT",
        default="mkv",
        help=(
            "File extension (without leading dot) of the re-encoded output "
            "videos. Default: %(default)s"
        ),
    )

    parser.add_argument(
        "--no-sidecar",
        action="store_true",
        default=False,
        help="Skip creating XMP sidecar files.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Log what would be done without making any changes.",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging verbosity. Default: %(default)s",
    )

    return parser


def parse_args() -> argparse.Namespace:
    return build_parser().parse_args()
