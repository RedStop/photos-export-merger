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
    frame_count: int
    audio_channels: int
    audio_codec: str | None
    total_bitrate_kbps: int = 0


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

    # Total (container) bitrate: prefer the format-level figure, else derive it
    # from the file size. Used for target/skip comparisons, which are all in
    # whole-file terms (video + audio + container overhead).
    total_bitrate_kbps = 0
    if fmt.get("bit_rate"):
        total_bitrate_kbps = round(int(fmt["bit_rate"]) / 1000)
    elif duration_sec > 0:
        try:
            total_bitrate_kbps = round(path.stat().st_size * 8 / duration_sec / 1000)
        except OSError:
            total_bitrate_kbps = 0

    # Actual frame count when ffprobe reports it; otherwise estimate from fps.
    frame_count = 0
    nb_frames = video_stream.get("nb_frames")
    if nb_frames and str(nb_frames).isdigit():
        frame_count = int(nb_frames)
    elif fps > 0 and duration_sec > 0:
        frame_count = round(fps * duration_sec)

    return VideoInfo(
        codec=video_stream.get("codec_name", "unknown"),
        width=int(video_stream.get("width", 0)),
        height=int(video_stream.get("height", 0)),
        fps=fps,
        is_vfr=is_vfr,
        bitrate_kbps=bitrate_kbps,
        duration_sec=duration_sec,
        frame_count=frame_count,
        audio_channels=audio_channels,
        audio_codec=audio_stream.get("codec_name") if audio_stream else None,
        total_bitrate_kbps=total_bitrate_kbps,
    )


def get_total_bitrate(path: Path, duration_hint: float | None = None) -> int:
    """Get the whole-file (container) bitrate of an encoded file in kbps.

    This is the figure target/window comparisons run against: it includes
    video, audio, and container overhead. Prefers the format-level bitrate and
    falls back to a file-size calculation. Returns -1 if it can't be determined.
    """
    try:
        probe = run_ffprobe(path)
    except (json.JSONDecodeError, subprocess.SubprocessError):
        return -1

    fmt = probe.get("format", {})
    if fmt.get("bit_rate"):
        return round(int(fmt["bit_rate"]) / 1000)

    duration = float(fmt.get("duration", 0)) or (duration_hint or 0)
    if duration <= 0:
        return -1
    try:
        file_size = path.stat().st_size
    except OSError:
        return -1
    return round(file_size * 8 / duration / 1000)


def _sum_video_packet_bytes(path: Path) -> int | None:
    """Sum the sizes of all video packets in a file, or None on failure.

    Reads packet sizes directly (``-show_entries packet=size``), which works
    even when the container omits a per-stream bitrate — as Matroska usually
    does for the streams ffmpeg writes.
    """
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "quiet",
                "-select_streams", "v:0",
                "-show_entries", "packet=size",
                "-of", "csv=p=0",
                str(path),
            ],
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None

    total = 0
    for line in result.stdout.splitlines():
        line = line.strip().rstrip(",")
        if line.isdigit():
            total += int(line)
    return total


def measure_overhead(path: Path, duration_hint: float | None = None) -> int | None:
    """Measure the audio + container overhead of an encoded file in kbps.

    Defined as ``total_bitrate - video_bitrate``, both measured from the file:
    the total from the container bitrate (or file size) and the video-only
    figure from summing the video packet sizes. For a file with no audio this
    is just the container muxing overhead (typically small). Returns None when
    either figure can't be determined.
    """
    try:
        probe = run_ffprobe(path)
    except (json.JSONDecodeError, subprocess.SubprocessError):
        return None

    fmt = probe.get("format", {})
    duration = float(fmt.get("duration", 0)) or (duration_hint or 0)
    if duration <= 0:
        return None

    if fmt.get("bit_rate"):
        total = int(fmt["bit_rate"]) / 1000
    else:
        try:
            total = path.stat().st_size * 8 / duration / 1000
        except OSError:
            return None

    video_bytes = _sum_video_packet_bytes(path)
    if video_bytes is None:
        return None
    video = video_bytes * 8 / duration / 1000
    return max(0, round(total - video))
