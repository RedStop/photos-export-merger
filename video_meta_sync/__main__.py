"""
Entry point for ``python -m video_meta_sync``.

Usage examples
--------------
  python -m video_meta_sync                          # current directory
  python -m video_meta_sync /path/to/videos
  python -m video_meta_sync /path/to/video.mkv
  python -m video_meta_sync --output-ext mp4 /path
  python -m video_meta_sync --no-sidecar /path
  python -m video_meta_sync --dry-run /path
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from .cli import parse_args
from .processor import process_pair
from .scanner import scan_directory, scan_single_file


def _configure_logging(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)s – %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )


def main() -> int:
    args = parse_args()
    _configure_logging(args.log_level)

    log = logging.getLogger(__name__)

    # ------------------------------------------------------------------ #
    # Resolve target path                                                  #
    # ------------------------------------------------------------------ #
    target: Path = args.target if args.target is not None else Path.cwd()

    if not target.exists():
        log.error("Target path does not exist: '%s'.", target)
        return 1

    output_ext: str = args.output_ext.lower().lstrip(".")

    # ------------------------------------------------------------------ #
    # Discover pairs                                                       #
    # ------------------------------------------------------------------ #
    if target.is_file():
        pair = scan_single_file(target, output_ext)
        pairs = [pair] if pair is not None else []
    elif target.is_dir():
        pairs = scan_directory(target, output_ext)
    else:
        log.error("Target '%s' is neither a file nor a directory.", target)
        return 1

    if not pairs:
        log.info("No re-encoded/original pairs found – nothing to do.")
        return 0

    # ------------------------------------------------------------------ #
    # Process each pair                                                    #
    # ------------------------------------------------------------------ #
    processed = 0
    skipped   = 0
    errors    = 0

    for pair in pairs:
        try:
            result = process_pair(
                pair,
                create_sidecar=not args.no_sidecar,
                dry_run=args.dry_run,
            )
            if result:
                processed += 1
            else:
                skipped += 1
        except Exception as exc:  # pragma: no cover
            log.exception(
                "Unexpected error processing '%s': %s", pair.reencoded, exc
            )
            errors += 1

    # ------------------------------------------------------------------ #
    # Summary                                                              #
    # ------------------------------------------------------------------ #
    log.info(
        "Done. processed=%d  skipped=%d  errors=%d",
        processed,
        skipped,
        errors,
    )
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
