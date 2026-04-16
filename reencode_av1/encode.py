"""FFmpeg encoding helpers: sample encodes, full encodes, and progress display."""

from __future__ import annotations

import logging
import re
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

from .probe import get_video_bitrate

log = logging.getLogger(__name__)

# Regex patterns for parsing ffmpeg progress output
_RE_PROGRESS = re.compile(
    r"frame=\s*(\d+).*?time=(\S+).*?speed=(\S+)"
)
_RE_BITRATE = re.compile(r"bitrate=\s*(\S+)")
_RE_SIZE = re.compile(r"size=\s*(\S+)")
_RE_FPS = re.compile(r"fps=\s*(\S+)")


def _base_encode_args(
    crf: int,
    extra_args: list[str],
    audio_bitrate: str,
    preset: int,
) -> list[str]:
    """Return the common codec/quality arguments shared by all encodes."""
    return [
        *extra_args,
        "-c:v", "libsvtav1",
        "-preset", str(preset),
        "-crf", str(crf),
        "-pix_fmt", "yuv420p10le",
        "-c:a", "libopus",
        "-b:a", audio_bitrate,
        "-vbr", "on",
        "-compression_level", "10",
    ]


def _make_temp_path() -> Path:
    """Create a unique temporary file path for an MKV encode."""
    return Path(tempfile.gettempdir()) / f"av1_sample_{uuid.uuid4().hex}.mkv"


def encode_sample(
    input_path: Path,
    crf: int,
    extra_args: list[str],
    audio_bitrate: str,
    preset: int,
    audio_bitrate_kbps: int,
    *,
    duration: float | None = None,
    keep_file: bool = False,
) -> tuple[int, Path | None]:
    """Encode a sample (or full video) at a given CRF.

    Args:
        input_path: source video file.
        crf: CRF value to use.
        extra_args: additional ffmpeg args (scaling, GOP, etc.).
        audio_bitrate: audio bitrate string like ``"128k"``.
        preset: SVT-AV1 preset.
        audio_bitrate_kbps: numeric audio bitrate for fallback calculation.
        duration: if set, only encode the first *duration* seconds.
        keep_file: if True, return the temp file path instead of deleting it.

    Returns:
        ``(bitrate_kbps, temp_path | None)``.  *temp_path* is only set when
        *keep_file* is True and the encode succeeded.
    """
    temp_path = _make_temp_path()

    time_args = ["-t", str(duration)] if duration is not None else []
    ff_args = [
        "ffmpeg",
        "-y", "-hide_banner", "-loglevel", "error",
        *time_args,
        "-i", str(input_path),
        *_base_encode_args(crf, extra_args, audio_bitrate, preset),
        str(temp_path),
    ]

    result = subprocess.run(ff_args, capture_output=True, text=True)

    if not temp_path.exists():
        stderr = result.stderr.strip()
        if stderr:
            log.warning("    ffmpeg error (CRF=%d): %s", crf, stderr)
        else:
            log.warning(
                "    ffmpeg produced no output (CRF=%d, exit=%d)",
                crf, result.returncode,
            )
        return -1, None

    bitrate = get_video_bitrate(temp_path, audio_bitrate_kbps)

    if keep_file and bitrate >= 0:
        return bitrate, temp_path
    if keep_file:
        log.warning(
            "    Could not determine bitrate for CRF=%d, returning file anyway",
            crf,
        )
        return -1, temp_path

    # Clean up
    temp_path.unlink(missing_ok=True)
    return bitrate, None


def _extract_vf_filter(extra_args: list[str]) -> tuple[str | None, list[str]]:
    """Separate any ``-vf`` filter from extra_args.

    Returns ``(vf_string_or_none, remaining_args)``.  This is needed
    because ``-vf`` and ``-filter_complex`` cannot coexist in the same
    ffmpeg command.
    """
    remaining: list[str] = []
    vf_filter: str | None = None
    skip_next = False
    for i, arg in enumerate(extra_args):
        if skip_next:
            skip_next = False
            continue
        if arg == "-vf" and i + 1 < len(extra_args):
            vf_filter = extra_args[i + 1]
            skip_next = True
        else:
            remaining.append(arg)
    return vf_filter, remaining


def encode_segments(
    input_path: Path,
    crf: int,
    extra_args: list[str],
    audio_bitrate: str,
    preset: int,
    audio_bitrate_kbps: int,
    offsets: list[float],
    seg_duration: float,
) -> int:
    """Encode multiple segments via concat and return the average video bitrate.

    Uses ffmpeg's concat filter by opening the input once per segment
    with ``-ss``/``-t``, then concatenating the streams. If *extra_args*
    contains a ``-vf`` scale filter it is folded into the filter_complex
    chain (since ``-vf`` and ``-filter_complex`` cannot coexist).
    """
    temp_path = _make_temp_path()

    # Separate -vf from extra_args so we can fold it into filter_complex
    vf_filter, remaining_args = _extract_vf_filter(extra_args)

    # Build input args: -ss <offset> -t <dur> -i <file> for each segment
    input_args: list[str] = []
    filter_parts: list[str] = []      # scale filter definitions only
    concat_inputs: list[str] = []     # labels fed to concat
    n = len(offsets)

    for i, offset in enumerate(offsets):
        input_args.extend([
            "-ss", f"{offset:.3f}",
            "-t", f"{seg_duration:.3f}",
            "-i", str(input_path),
        ])

        if vf_filter:
            # Apply the scale filter per-stream before concat
            filter_parts.append(f"[{i}:v]{vf_filter}[v{i}];")
            concat_inputs.append(f"[v{i}]")
        else:
            concat_inputs.append(f"[{i}:v]")

        concat_inputs.append(f"[{i}:a]")

    # Construct filter_complex
    filter_complex = (
        "".join(filter_parts)
        + "".join(concat_inputs)
        + f"concat=n={n}:v=1:a=1[outv][outa]"
    )

    ff_args = [
        "ffmpeg",
        "-y", "-hide_banner", "-loglevel", "error",
        *input_args,
        "-filter_complex", filter_complex,
        "-map", "[outv]", "-map", "[outa]",
        *_base_encode_args(crf, remaining_args, audio_bitrate, preset),
        str(temp_path),
    ]

    # Log the complete command at DEBUG level (always)
    cmd_str = " ".join(ff_args)
    log.debug("FFmpeg command: %s", cmd_str)

    try:
        result = subprocess.run(ff_args, capture_output=True, text=True)

        if not temp_path.exists():
            stderr = result.stderr.strip()
            if stderr:
                # If DEBUG logging is disabled, emit the command at WARNING
                # level before the error message (per request)
                if not log.isEnabledFor(logging.DEBUG):
                    log.warning("    FFmpeg command: %s", cmd_str)
                log.warning("    Segment encode error (CRF=%d): %s", crf, stderr)
            return -1

        bitrate = get_video_bitrate(temp_path, audio_bitrate_kbps)
        return bitrate
    finally:
        temp_path.unlink(missing_ok=True)


def encode_full(
    input_path: Path,
    output_path: Path,
    crf: int,
    extra_args: list[str],
    audio_bitrate: str,
    preset: int,
    duration_sec: float,
) -> int:
    """Encode the full video with live progress display.

    Returns the ffmpeg exit code.
    """
    ff_args = [
        "-y", "-hide_banner", "-stats",
        "-i", str(input_path),
        *_base_encode_args(crf, extra_args, audio_bitrate, preset),
        str(output_path),
    ]

    cmd_str = "ffmpeg " + " ".join(
        f'"{a}"' if " " in a else a for a in ff_args
    )
    log.info("  Command: %s", cmd_str)

    return _run_ffmpeg_with_progress(ff_args, duration_sec)


def _run_ffmpeg_with_progress(ff_args: list[str], total_duration: float) -> int:
    """Run ffmpeg and display a live progress line on stderr."""
    proc = subprocess.Popen(
        ["ffmpeg", *ff_args],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )

    last_len = 0
    assert proc.stderr is not None

    for line in proc.stderr:
        line = line.rstrip()
        if "frame=" not in line or "time=" not in line:
            continue

        m_progress = _RE_PROGRESS.search(line)
        if not m_progress:
            continue

        frame = m_progress.group(1)
        time_str = m_progress.group(2)
        speed = m_progress.group(3)

        m_fps = _RE_FPS.search(line)
        enc_fps = m_fps.group(1) if m_fps else "?"

        m_br = _RE_BITRATE.search(line)
        bitrate_str = m_br.group(1) if m_br else "?"

        m_sz = _RE_SIZE.search(line)
        size_str = m_sz.group(1) if m_sz else "?"

        pct_str = ""
        if total_duration > 0:
            pct = _parse_time_to_seconds(time_str) / total_duration * 100
            pct = min(100.0, pct)
            pct_str = f" {pct:.1f}%"

        status = (
            f"  [ENCODE]{pct_str} time={time_str} frame={frame} "
            f"fps={enc_fps} bitrate={bitrate_str} size={size_str} speed={speed}"
        )
        padded = status.ljust(last_len)
        sys.stderr.write(f"\r{padded}")
        sys.stderr.flush()
        last_len = len(status)

    proc.wait()

    if last_len > 0:
        sys.stderr.write(f"\r{' ' * last_len}\r")
        sys.stderr.flush()

    return proc.returncode


def _parse_time_to_seconds(time_str: str) -> float:
    """Parse an ffmpeg time string like ``00:01:23.45`` into seconds."""
    parts = re.split(r"[:.]", time_str)
    if len(parts) < 3:
        return 0.0
    seconds = float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
    if len(parts) >= 4:
        seconds += float(f"0.{parts[3]}")
    return seconds
