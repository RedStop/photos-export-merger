"""Entry point: CLI parsing, validation, and main processing loop."""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
import time
from pathlib import Path

from .encode import encode_full
from .filters import build_extra_args, compute_segment_offsets, compute_windows
from .probe import VideoInfo, get_video_bitrate, get_video_info
from .search import find_optimal_crf, find_optimal_crf_interpolated

log = logging.getLogger("reencode_av1")

VIDEO_EXTENSIONS = frozenset((
    ".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv",
    ".webm", ".m4v", ".ts", ".mpg", ".mpeg", ".3gp",
))
SKIP_CODECS = frozenset(("av1", "vp9"))


# ── CLI ──────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="reencode_av1",
        description="Batch re-encode videos to AV1 (SVT-AV1) with automatic CRF tuning.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  python -m reencode_av1                              # default settings
  python -m reencode_av1 --target-bitrate 2000        # lower target
  python -m reencode_av1 --dry-run                    # preview only
  python -m reencode_av1 --interpolate                # use interpolation
  python -m reencode_av1 --precise                    # full-video search if out of range
""",
    )

    p.add_argument(
        "--target-bitrate", type=int, default=2500,
        help="Target video bitrate in kbps (default: 2500)",
    )
    p.add_argument(
        "--allowed-bitrate-window", type=int, default=500,
        help="Acceptable final bitrate range below target (default: 500)",
    )
    p.add_argument(
        "--target-bitrate-window", type=int, default=100,
        help="Immediate-accept zone width below target for full encodes (default: 100)",
    )
    p.add_argument(
        "--sample-bitrate-window-buffer", type=int, default=None,
        help="Inward buffer narrowing the sample search window (default: half of target-bitrate-window)",
    )
    p.add_argument(
        "--crf-min", type=int, default=1,
        help="Minimum CRF value (default: 1)",
    )
    p.add_argument(
        "--crf-max", type=int, default=63,
        help="Maximum CRF value (default: 63)",
    )
    p.add_argument(
        "--max-iterations", type=int, default=15,
        help="Maximum binary search iterations (default: 15)",
    )
    p.add_argument(
        "--segment-count", type=int, default=5,
        help="Number of sample segments (default: 5)",
    )
    p.add_argument(
        "--segment-duration", type=float, default=3.0,
        help="Duration of each sample segment in seconds (default: 3.0)",
    )
    p.add_argument(
        "--audio-bitrate", type=int, default=0,
        help="Opus audio bitrate in kbps; 0 = auto 64k/channel (default: 0)",
    )
    p.add_argument(
        "--preset", type=int, default=3,
        help="SVT-AV1 preset 0-13 (default: 3)",
    )
    p.add_argument(
        "--interpolate", action="store_true",
        help="Use log-linear CRF interpolation instead of pure binary search",
    )
    p.add_argument(
        "--precise", action="store_true",
        help="Redo search with full video if final bitrate is out of range",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Log what would be done without encoding",
    )
    p.add_argument(
        "--log-file", type=str, default="reencode-av1.log",
        help="Path to log file (default: reencode-av1.log)",
    )
    p.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable debug logging",
    )

    return p


def validate_args(args: argparse.Namespace) -> None:
    """Validate CLI arguments and exit with a clear message on error."""
    errors: list[str] = []

    if args.target_bitrate <= 0:
        errors.append("--target-bitrate must be positive")

    if args.allowed_bitrate_window <= 0:
        errors.append("--allowed-bitrate-window must be positive")
    elif args.allowed_bitrate_window >= args.target_bitrate:
        errors.append("--allowed-bitrate-window must be less than --target-bitrate")

    if args.target_bitrate_window <= 0:
        errors.append("--target-bitrate-window must be positive")
    elif args.target_bitrate_window > args.allowed_bitrate_window:
        errors.append(
            "--target-bitrate-window must be <= --allowed-bitrate-window"
        )

    # Resolve default for sample buffer
    if args.sample_bitrate_window_buffer is None:
        args.sample_bitrate_window_buffer = args.target_bitrate_window // 2

    buf = args.sample_bitrate_window_buffer
    if buf < 0:
        errors.append("--sample-bitrate-window-buffer must be >= 0")

    # Check sample window has positive length: allowed - 2*buffer > 0
    sample_width = args.allowed_bitrate_window - 2 * buf
    if sample_width <= 0:
        errors.append(
            f"Sample window has zero or negative width: "
            f"allowed_window ({args.allowed_bitrate_window}) - 2*buffer ({2 * buf}) = {sample_width}. "
            f"Reduce --sample-bitrate-window-buffer or increase --allowed-bitrate-window."
        )

    # Check confident zone: target_window >= buffer
    if args.target_bitrate_window < buf:
        errors.append(
            f"Confident sample zone is inverted: "
            f"target_window ({args.target_bitrate_window}) < buffer ({buf}). "
            f"Increase --target-bitrate-window or decrease --sample-bitrate-window-buffer."
        )

    if not (0 <= args.crf_min < args.crf_max <= 63):
        errors.append("--crf-min and --crf-max must satisfy 0 <= min < max <= 63")

    if not (0 <= args.preset <= 13):
        errors.append("--preset must be in [0, 13]")

    if args.segment_count < 1:
        errors.append("--segment-count must be >= 1")

    if args.segment_duration <= 0:
        errors.append("--segment-duration must be > 0")

    if errors:
        for e in errors:
            print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


# ── Logging setup ────────────────────────────────────────────────────────────

def setup_logging(log_file: str, verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    root = logging.getLogger("reencode_av1")
    root.setLevel(level)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    root.addHandler(ch)

    # File handler
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    root.addHandler(fh)


# ── Helpers ──────────────────────────────────────────────────────────────────

def get_output_path(input_path: Path) -> Path:
    """Compute the output path for a given input video."""
    if input_path.suffix.lower() == ".mkv":
        return input_path.with_stem(input_path.stem + "-reencoded")
    return input_path.with_suffix(".mkv")


def compute_audio_bitrate(info: VideoInfo, override_kbps: int) -> tuple[str, int]:
    """Return the audio bitrate as a string and int.

    Auto mode: 64 kbps per channel.
    """
    if override_kbps > 0:
        return f"{override_kbps}k", override_kbps
    auto_kbps = info.audio_channels * 64
    return f"{auto_kbps}k", auto_kbps


# ── Main loop ────────────────────────────────────────────────────────────────

def process_file(
    input_path: Path,
    args: argparse.Namespace,
) -> str:
    """Process a single video file.

    Returns ``"processed"``, ``"skipped"``, or ``"failed"``.
    """
    info = get_video_info(input_path)
    if info is None:
        log.warning("  Could not read video info, skipping")
        return "skipped"

    log.info(
        "  Codec=%s Resolution=%dx%d FPS=%.3f Bitrate=%d kbps AudioCh=%d",
        info.codec, info.width, info.height, info.fps,
        info.bitrate_kbps, info.audio_channels,
    )

    # Skip AV1 / VP9
    if info.codec in SKIP_CODECS:
        log.info("  Already encoded as %s, skipping", info.codec)
        return "skipped"

    # Check output
    output_path = get_output_path(input_path)
    if output_path.exists():
        log.info("  Output already exists: %s, skipping", output_path)
        return "skipped"

    # Log VFR / non-30fps
    if info.is_vfr:
        log.warning("  Variable frame rate detected")
    if info.fps > 0 and abs(info.fps - 30.0) > 1.0:
        log.info("  Video is %.3f fps (not 30 fps)", info.fps)

    # Audio bitrate
    audio_str, audio_kbps = compute_audio_bitrate(info, args.audio_bitrate)
    log.info("  Audio: %d channel(s) -> Opus %s", info.audio_channels, audio_str)

    # Effective target (never increase bitrate)
    effective_target = args.target_bitrate
    if 0 < info.bitrate_kbps < args.target_bitrate:
        effective_target = info.bitrate_kbps
        log.info(
            "  Original bitrate (%d kbps) below target, using it instead",
            info.bitrate_kbps,
        )

    # Compute windows with effective target
    windows = compute_windows(
        effective_target,
        args.allowed_bitrate_window,
        args.target_bitrate_window,
        args.sample_bitrate_window_buffer,
    )

    extra_args = build_extra_args(info)

    if args.dry_run:
        log.info("  [DRY RUN] Would encode to: %s", output_path)
        return "skipped"

    # Determine if short video
    total_sample_time = args.segment_count * args.segment_duration
    is_short_video = info.duration_sec > 0 and info.duration_sec <= total_sample_time

    if is_short_video:
        log.info(
            "  Video (%.1fs) <= total sample time (%.1fs), encoding full video during search",
            info.duration_sec, total_sample_time,
        )

    # Choose search function
    search_fn = find_optimal_crf_interpolated if args.interpolate else find_optimal_crf

    # Compute segment offsets for multi-segment sampling
    offsets: list[float] | None = None
    if not is_short_video:
        offsets = compute_segment_offsets(
            info.duration_sec, args.segment_count, args.segment_duration,
        )
        log.debug("  Segment offsets: %s", offsets)

    log.info("  Starting CRF search%s...", " (interpolation)" if args.interpolate else "")

    # Track temp files for cleanup on exceptions
    temp_files: list[Path] = []

    try:
        result = search_fn(
            input_path, windows, extra_args, audio_str,
            args.preset, audio_kbps, args.max_iterations,
            args.crf_min, args.crf_max,
            offsets=offsets, seg_duration=args.segment_duration,
            full_encode=is_short_video,
        )
        if result.temp_file:
            temp_files.append(result.temp_file)

        # ── Full encode ──────────────────────────────────────────────
        if is_short_video and result.temp_file and result.temp_file.exists():
            log.info(
                "  Reusing search encode (CRF=%d) as final output -> %s",
                result.crf, output_path,
            )
            start = time.monotonic()
            shutil.move(str(result.temp_file), str(output_path))
            elapsed = time.monotonic() - start
        else:
            # Clean up temp file if present
            if result.temp_file and result.temp_file.exists():
                result.temp_file.unlink(missing_ok=True)

            log.info("  Full encode with CRF=%d -> %s", result.crf, output_path)
            start = time.monotonic()
            exit_code = encode_full(
                input_path, output_path, result.crf, extra_args,
                audio_str, args.preset, info.duration_sec,
            )
            elapsed = time.monotonic() - start

            if exit_code != 0:
                log.error("  FAILED: ffmpeg exited with code %d", exit_code)
                return "failed"

        if not output_path.exists():
            log.error("  FAILED: output file was not created")
            return "failed"

        # ── Verify output ────────────────────────────────────────────
        out_bitrate = get_video_bitrate(output_path, audio_kbps)
        in_size_mb = input_path.stat().st_size / (1024 * 1024)
        out_size_mb = output_path.stat().st_size / (1024 * 1024)

        elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))

        if out_bitrate < 0:
            log.warning(
                "  Done in %s. %.1f MB -> %.1f MB (could not determine output bitrate)",
                elapsed_str, in_size_mb, out_size_mb,
            )
            return "processed"

        log.info(
            "  Done in %s. %.1f MB -> %.1f MB, output bitrate=%d kbps",
            elapsed_str, in_size_mb, out_size_mb, out_bitrate,
        )

        out_of_range = out_bitrate > windows.final_hi or out_bitrate < windows.final_lo

        if out_bitrate > windows.final_hi:
            log.warning(
                "  Final bitrate (%d kbps) exceeds target (%d kbps)",
                out_bitrate, windows.final_hi,
            )
        elif out_bitrate < windows.final_lo:
            log.warning(
                "  Final bitrate (%d kbps) below allowed range [%d, %d] kbps",
                out_bitrate, windows.final_lo, windows.final_hi,
            )

        # ── Precise mode ─────────────────────────────────────────────
        if args.precise and out_of_range and not is_short_video:
            log.info("  --precise: final bitrate out of range, starting full-video search...")
            output_path.unlink(missing_ok=True)

            precise_search_fn = (
                find_optimal_crf_interpolated if args.interpolate else find_optimal_crf
            )
            precise_result = precise_search_fn(
                input_path, windows, extra_args, audio_str,
                args.preset, audio_kbps, args.max_iterations,
                args.crf_min, args.crf_max,
                full_encode=True,
                seed_crf=result.crf,
            )
            if precise_result.temp_file:
                temp_files.append(precise_result.temp_file)

            if precise_result.temp_file and precise_result.temp_file.exists():
                shutil.move(str(precise_result.temp_file), str(output_path))
                final_bitrate = get_video_bitrate(output_path, audio_kbps)
                final_size_mb = output_path.stat().st_size / (1024 * 1024)
                log.info(
                    "  Precise encode (CRF=%d): %.1f MB -> %.1f MB, bitrate=%d kbps",
                    precise_result.crf, in_size_mb, final_size_mb, final_bitrate,
                )
                if final_bitrate > windows.final_hi:
                    log.warning(
                        "  Precise bitrate (%d kbps) still exceeds target (%d kbps), keeping anyway",
                        final_bitrate, windows.final_hi,
                    )
                elif final_bitrate < windows.final_lo:
                    log.warning(
                        "  Precise bitrate (%d kbps) still below range, keeping anyway",
                        final_bitrate,
                    )
            else:
                log.error("  Full-video search produced no usable file")
                return "failed"

        return "processed"

    finally:
        # Clean up any temp files that weren't moved to the output path
        for tmp in temp_files:
            try:
                if tmp.exists():
                    tmp.unlink(missing_ok=True)
            except OSError:
                pass


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(args)
    setup_logging(args.log_file, args.verbose)

    # Check for ffmpeg/ffprobe
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        log.error("ffmpeg and ffprobe must be on PATH")
        sys.exit(1)

    cwd = Path.cwd()
    log.info("=" * 60)
    log.info("AV1 Re-encode Session Started")
    log.info("=" * 60)
    log.info("Target: %d kbps", args.target_bitrate)
    log.info("Allowed window: %d kbps", args.allowed_bitrate_window)
    log.info("Target window: %d kbps", args.target_bitrate_window)
    log.info("Sample buffer: %d kbps", args.sample_bitrate_window_buffer)

    windows = compute_windows(
        args.target_bitrate,
        args.allowed_bitrate_window,
        args.target_bitrate_window,
        args.sample_bitrate_window_buffer,
    )
    log.info(
        "Sample window: [%d, %d], confident: [%d, %d]",
        windows.sample_lo, windows.sample_hi,
        windows.sample_confident_lo, windows.sample_confident_hi,
    )
    log.info(
        "Final window: [%d, %d], accept: [%d, %d]",
        windows.final_lo, windows.final_hi,
        windows.final_accept_lo, windows.final_accept_hi,
    )
    log.info("Scanning: %s", cwd)

    video_files = sorted(
        f for f in cwd.rglob("*")
        if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS
    )

    total = len(video_files)
    log.info("Found %d video file(s)", total)

    processed = skipped = failed = 0

    for i, file_path in enumerate(video_files, 1):
        log.info("-" * 60)
        log.info("Processing [%d/%d]: %s", i, total, file_path)

        result = process_file(file_path, args)
        if result == "processed":
            processed += 1
        elif result == "skipped":
            skipped += 1
        else:
            failed += 1

    log.info("=" * 60)
    log.info("Session Complete")
    log.info(
        "Processed: %d | Skipped: %d | Failed: %d | Total: %d",
        processed, skipped, failed, total,
    )


if __name__ == "__main__":
    main()
