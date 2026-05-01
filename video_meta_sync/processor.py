"""
Processor: orchestrates all steps for a single (re-encoded, original) pair.

Steps
-----
1. Check whether a sidecar already exists → skip everything if so.
2. Extract metadata from the original video.
3. Optionally embed metadata into the re-encoded file (writable containers).
4. Optionally write the XMP sidecar.
5. Update filesystem timestamps on the re-encoded file.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .embedder import embed_metadata
from .file_dates import update_file_dates
from .metadata import extract_metadata
from .scanner import VideoPair
from .xmp import sidecar_exists, write_sidecar

log = logging.getLogger(__name__)


def process_pair(
    pair: VideoPair,
    *,
    create_sidecar: bool = True,
    dry_run: bool = False,
) -> bool:
    """
    Process one (re-encoded, original) pair.

    Parameters
    ----------
    pair:
        The matched video pair.
    create_sidecar:
        When True (default) an XMP sidecar is written.
    dry_run:
        When True nothing is written; actions are only logged.

    Returns
    -------
    True  – pair was processed (or would be in dry-run).
    False – pair was skipped.
    """
    reencoded = pair.reencoded
    original  = pair.original

    log.info("Processing pair: '%s' ← '%s'.", reencoded.name, original.name)

    # ------------------------------------------------------------------
    # Guard: skip if sidecar already exists.
    # ------------------------------------------------------------------
    if sidecar_exists(reencoded):
        sidecar_path = reencoded.parent / (reencoded.name + ".xmp")
        log.info(
            "Sidecar '%s' already exists – skipping all updates for '%s'.",
            sidecar_path.name,
            reencoded.name,
        )
        return False

    # ------------------------------------------------------------------
    # Step 1: extract metadata from the original.
    # ------------------------------------------------------------------
    meta = extract_metadata(original)
    if meta is None:
        log.error(
            "Could not extract metadata from '%s' – skipping pair.", original
        )
        return False

    log.info(
        "Original time taken: %s  (recording tz: %s)",
        meta.original_time_taken.isoformat(),
        meta.recording_tz,
    )

    # ------------------------------------------------------------------
    # Step 2: embed metadata into the re-encoded file (if supported).
    # ------------------------------------------------------------------
    embed_metadata(reencoded, meta, dry_run=dry_run)

    # ------------------------------------------------------------------
    # Step 3: write XMP sidecar.
    # ------------------------------------------------------------------
    if create_sidecar:
        write_sidecar(reencoded, meta, original, dry_run=dry_run)
    else:
        log.debug("Sidecar creation skipped (--no-sidecar).")

    # ------------------------------------------------------------------
    # Step 4: update filesystem timestamps.
    # ------------------------------------------------------------------
    update_file_dates(reencoded, meta, dry_run=dry_run)

    return True
