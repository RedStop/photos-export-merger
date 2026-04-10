"""Video filter helpers: scaling, GOP settings, and segment calculations."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .probe import VideoInfo

log = logging.getLogger(__name__)


def build_extra_args(info: VideoInfo) -> list[str]:
    """Build extra ffmpeg arguments for scaling and GOP settings."""
    args: list[str] = []

    # ── Scaling (downscale above 1080p, never upscale) ───────────────
    scale_filter = _get_scale_filter(info.width, info.height)
    if scale_filter:
        args.extend(["-vf", scale_filter])
        log.info("  Downscaling with: %s", scale_filter)

    # ── GOP / keyframe interval (~8s / ~4s) ──────────────────────────
    fps = info.fps if info.fps > 0 else 30.0
    gop_size = round(fps * 8)
    keyint_min = round(fps * 4)
    args.extend(["-g", str(gop_size), "-keyint_min", str(keyint_min)])

    return args


def _get_scale_filter(width: int, height: int) -> str | None:
    """Return an ffmpeg scale filter string, or None if no scaling needed."""
    if width > height:
        # Landscape
        if height > 1080:
            return "scale=-2:1080"
        if width > 1920:
            return "scale=1920:-2"
    else:
        # Portrait or square
        if width > 1080:
            return "scale=1080:-2"
        if height > 1920:
            return "scale=-2:1920"
    return None


def compute_segment_offsets(
    duration: float,
    count: int,
    seg_duration: float,
) -> list[float]:
    """Compute evenly-spaced segment start times across a video.

    Places ``count`` segments at equal intervals through the video,
    ensuring no segment overshoots the end.

    Returns a list of start-time offsets in seconds.
    """
    offsets: list[float] = []
    for i in range(1, count + 1):
        centre = duration * i / (count + 1)
        start = centre - seg_duration / 2
        start = max(0.0, min(start, duration - seg_duration))
        offsets.append(round(start, 3))
    return offsets


@dataclass
class BitrateWindows:
    """Pre-computed bitrate acceptance windows.

    Attributes:
        target: the effective target bitrate (never exceed).
        sample_lo: lower bound for sample acceptance.
        sample_hi: upper bound for sample acceptance.
        sample_confident_lo: lower bound for immediate sample acceptance.
        sample_confident_hi: upper bound for immediate sample acceptance
                             (same as sample_hi).
        final_lo: lower bound for final encode acceptance.
        final_hi: upper bound for final encode acceptance (same as target).
        final_accept_lo: lower bound for immediate final acceptance.
        final_accept_hi: upper bound for immediate final acceptance
                         (same as target).
    """

    target: int
    sample_lo: int
    sample_hi: int
    sample_confident_lo: int
    sample_confident_hi: int
    final_lo: int
    final_hi: int
    final_accept_lo: int
    final_accept_hi: int


def compute_windows(
    target: int,
    allowed_window: int,
    target_window: int,
    sample_buffer: int,
) -> BitrateWindows:
    """Compute all bitrate acceptance windows from CLI parameters.

    Lower bounds are clamped to 0 so that a low effective target
    (e.g. when the source bitrate is already below the CLI target)
    doesn't produce negative window boundaries.
    """
    return BitrateWindows(
        target=target,
        sample_lo=max(0, target - allowed_window + sample_buffer),
        sample_hi=max(0, target - sample_buffer),
        sample_confident_lo=max(0, target - target_window),
        sample_confident_hi=max(0, target - sample_buffer),
        final_lo=max(0, target - allowed_window),
        final_hi=target,
        final_accept_lo=max(0, target - target_window),
        final_accept_hi=target,
    )
