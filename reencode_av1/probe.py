"""Video probing utilities using ffprobe."""

from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass
class VideoInfo:
    """Metadata extracted from a video file."""

    codec: str
    width: int
    height: int
    fps: float
    is_vfr: bool
    bitrate_kbps: int
    duration_sec: float
    audio_channels: int
    audio_codec: str | None


def run_ffprobe(path: Path) -> dict:
    """Run ffprobe and return the parsed JSON output."""
    result = subprocess.run(
        [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-show_format",
            str(path),
        ],
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def _parse_fraction(fraction: str) -> float:
    """Parse a fractional string like '30000/1001' into a float."""
    parts = fraction.split("/")
    if len(parts) == 2:
        denom = float(parts[1])
        if denom != 0:
            return float(parts[0]) / denom
    return 0.0


def get_video_info(path: Path) -> VideoInfo | None:
    """Probe a video file and return its metadata, or None on failure."""
    try:
        probe = run_ffprobe(path)
    except (json.JSONDecodeError, subprocess.SubprocessError) as exc:
        log.warning("ffprobe failed for %s: %s", path, exc)
        return None

    streams = probe.get("streams", [])
    fmt = probe.get("format", {})

    video_stream = next(
        (s for s in streams if s.get("codec_type") == "video"), None
    )
    audio_stream = next(
        (s for s in streams if s.get("codec_type") == "audio"), None
    )

    if video_stream is None:
        return None

    # Frame rate
    fps = 0.0
    avg_fr = video_stream.get("avg_frame_rate", "0/0")
    if avg_fr and avg_fr != "0/0":
        fps = round(_parse_fraction(avg_fr), 3)

    # VFR detection
    is_vfr = False
    r_fr = video_stream.get("r_frame_rate", "")
    if r_fr and avg_fr:
        r_fps = round(_parse_fraction(r_fr), 3)
        if r_fps > 0 and fps > 0 and abs(r_fps - fps) > 0.5:
            is_vfr = True

    # Bitrate: prefer stream, fall back to format minus audio estimate
    bitrate_kbps = 0
    if video_stream.get("bit_rate"):
        bitrate_kbps = round(int(video_stream["bit_rate"]) / 1000)
    elif fmt.get("bit_rate"):
        total = int(fmt["bit_rate"]) / 1000
        audio_est = 0
        if audio_stream and audio_stream.get("bit_rate"):
            audio_est = int(audio_stream["bit_rate"]) / 1000
        elif audio_stream:
            audio_est = 128
        bitrate_kbps = round(total - audio_est)

    audio_channels = 2
    if audio_stream and audio_stream.get("channels"):
        audio_channels = int(audio_stream["channels"])

    duration_sec = float(fmt.get("duration", 0))

    return VideoInfo(
        codec=video_stream.get("codec_name", "unknown"),
        width=int(video_stream.get("width", 0)),
        height=int(video_stream.get("height", 0)),
        fps=fps,
        is_vfr=is_vfr,
        bitrate_kbps=bitrate_kbps,
        duration_sec=duration_sec,
        audio_channels=audio_channels,
        audio_codec=audio_stream.get("codec_name") if audio_stream else None,
    )


def get_video_bitrate(path: Path, audio_bitrate_kbps: int) -> int:
    """Get the video-only bitrate of an encoded file in kbps.

    Falls back to computing from file size if the stream bitrate is missing.
    """
    try:
        probe = run_ffprobe(path)
    except (json.JSONDecodeError, subprocess.SubprocessError):
        return -1

    streams = probe.get("streams", [])
    fmt = probe.get("format", {})

    video_stream = next(
        (s for s in streams if s.get("codec_type") == "video"), None
    )

    if video_stream and video_stream.get("bit_rate"):
        return round(int(video_stream["bit_rate"]) / 1000)

    # Fallback: file size based calculation
    duration = float(fmt.get("duration", 0))
    if duration <= 0:
        return -1

    file_size = path.stat().st_size  # bytes
    audio_bits = audio_bitrate_kbps * 1000 * duration
    video_bits = (file_size * 8) - audio_bits
    return round(video_bits / duration / 1000)
