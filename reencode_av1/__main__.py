"""Entry point: CLI parsing, validation, and main processing loop."""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
import time
import uuid
from pathlib import Path

from .encode import encode_full
from .filters import build_extra_args, compute_segment_offsets, compute_windows
from .probe import VideoInfo, get_video_bitrate, get_video_info
from .search import (
    CrfPoint,
    binary_search_next,
    find_optimal_crf,
    interpolation_next,
    smart_search_next,
)

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
  python -m reencode_av1                               # current directory
  python -m reencode_av1 /path/to/videos               # specific directory
  python -m reencode_av1 --target-bitrate 2000         # lower target
  python -m reencode_av1 --dry-run                     # preview only
  python -m reencode_av1 --search-method binary        # use pure binary search
  python -m reencode_av1 --search-method interpolation # use log-linear interpolation
  python -m reencode_av1 --precise                     # full-video search if out of range
  python -m reencode_av1 --scratch-dir D:/scratch \\nas/videos  # stage NAS sources on local disk
  python -m reencode_av1 --skip-below-bitrate 1000     # skip files already under 1000 kbps
  python -m reencode_av1 --skip-below-bitrate 0        # always encode regardless of bitrate
  python -m reencode_av1 --crf-max 55                  # tighter quality floor
  python -m reencode_av1 --crf-ceiling-fallback 52     # specific fallback CRF when crf-max exceeded
""",
    )

    p.add_argument(
        "--target-bitrate", type=int, default=2500,
        help="Target video bitrate in kbps (default: 2500)",
    )
    p.add_argument(
        "--skip-below-bitrate", type=int, default=None,
        help=(
            "Skip re-encoding if the source video bitrate is already at or below this "
            "value in kbps. Defaults to --target-bitrate when not set. "
            "Set to 0 to disable and always encode."
        ),
    )
    p.add_argument(
        "--accept-window", type=int, default=1500,
        help="Acceptable final bitrate range below target (default: 1500)",
    )
    p.add_argument(
        "--confident-window", type=int, default=200,
        help="Immediate-accept zone width below target for full encodes (default: 200)",
    )
    p.add_argument(
        "--sample-window-buffer", type=int, default=None,
        help="Inward buffer narrowing the sample search window (default: quarter of confident-window)",
    )
    p.add_argument(
        "--crf-min", type=int, default=15,
        help="Minimum CRF value (default: 15)",
    )
    p.add_argument(
        "--crf-max", type=int, default=57,
        help=(
            "Maximum CRF value for the search. If the bitrate at this CRF is still "
            "above the target, the video is encoded with --crf-ceiling-fallback instead "
            "(default: 57)."
        ),
    )
    p.add_argument(
        "--crf-ceiling-fallback", type=int, default=None,
        help=(
            "CRF to use when the search reaches --crf-max and the bitrate is still "
            "above the target. Defaults to --crf-max when not set, but 48 is recommended."
        ),
    )
    p.add_argument(
        "--max-iterations", type=int, default=15,
        help="Maximum CRF search iterations (default: 15)",
    )
    p.add_argument(
        "--segment-count", type=int, default=5,
        help="Number of sample segments (default: 5)",
    )
    p.add_argument(
        "--segment-duration", type=float, default=5.0,
        help="Duration of each sample segment in seconds (default: 5.0)",
    )
    p.add_argument(
        "--short-video-threshold", type=float, default=90.0,
        help=(
            "Videos at or shorter than this duration (in seconds) are encoded in full "
            "during the CRF search instead of using segment sampling (default: 90.0). "
            "Must be at least double segment-count * segment-duration."
        ),
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
        "--search-method", choices=("smart", "interpolation", "binary"),
        default="smart",
        help=(
            "CRF search strategy: 'smart' (default, bracket-then-interpolate), "
            "'interpolation' (log-linear), or 'binary' (pure binary search)"
        ),
    )
    p.add_argument(
        "--precise", action="store_true",
        help="Redo search with full video if final bitrate is out of range",
    )
    p.add_argument(
        "--scratch-dir", type=Path, default=None,
        help=(
            "Local scratch directory. When set, each source is copied here before "
            "encoding so the many decode passes read from local disk instead of the "
            "source location (e.g. a NAS share); the final output is written here and "
            "moved to its destination once complete. The directory is created if needed."
        ),
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
    p.add_argument(
        "directory", nargs="?", type=Path, default=None,
        help="Directory to scan for video files (default: current directory)",
    )

    return p


def validate_args(args: argparse.Namespace) -> None:
    """Validate CLI arguments and exit with a clear message on error."""
    errors: list[str] = []

    if args.target_bitrate <= 0:
        errors.append("--target-bitrate must be positive")

    if args.accept_window <= 0:
        errors.append("--accept-window must be positive")
    elif args.accept_window >= args.target_bitrate:
        errors.append("--accept-window must be less than --target-bitrate")

    if args.confident_window <= 0:
        errors.append("--confident-window must be positive")
    elif args.confident_window > args.accept_window:
        errors.append(
            "--confident-window must be <= --accept-window"
        )

    # Resolve default for skip-below-bitrate
    if args.skip_below_bitrate is None:
        args.skip_below_bitrate = args.target_bitrate

    if args.skip_below_bitrate < 0:
        errors.append("--skip-below-bitrate must be >= 0 (use 0 to disable)")

    # Resolve default for sample buffer
    if args.sample_window_buffer is None:
        args.sample_window_buffer = args.confident_window // 4

    buf = args.sample_window_buffer
    if buf < 0:
        errors.append("--sample-window-buffer must be >= 0")

    # Check sample window has positive length: accept - 2*buffer > 0
    sample_width = args.accept_window - 2 * buf
    if sample_width <= 0:
        errors.append(
            f"Sample window has zero or negative width: "
            f"accept_window ({args.accept_window}) - 2*buffer ({2 * buf}) = {sample_width}. "
            f"Reduce --sample-window-buffer or increase --accept-window."
        )

    # Check confident zone: confident_window >= 2*buffer
    if args.confident_window < 2 * buf:
        errors.append(
            f"Confident sample zone is inverted: "
            f"confident_window ({args.confident_window}) < 2*buffer ({2 * buf}). "
            f"Increase --confident-window or decrease --sample-window-buffer."
        )

    if not (0 <= args.crf_min < args.crf_max <= 63):
        errors.append("--crf-min and --crf-max must satisfy 0 <= min < max <= 63")

    # Resolve default for crf-ceiling-fallback
    if args.crf_ceiling_fallback is None:
        args.crf_ceiling_fallback = args.crf_max

    if not (args.crf_min <= args.crf_ceiling_fallback <= args.crf_max):
        errors.append(
            f"--crf-ceiling-fallback ({args.crf_ceiling_fallback}) must be within "
            f"[--crf-min, --crf-max] ([{args.crf_min}, {args.crf_max}])"
        )

    if not (0 <= args.preset <= 13):
        errors.append("--preset must be in [0, 13]")

    if args.segment_count < 1:
        errors.append("--segment-count must be >= 1")

    if args.segment_duration <= 0:
        errors.append("--segment-duration must be > 0")

    if args.short_video_threshold <= 0:
        errors.append("--short-video-threshold must be > 0")
    else:
        min_threshold = 2.0 * args.segment_count * args.segment_duration
        if args.short_video_threshold < min_threshold:
            errors.append(
                f"--short-video-threshold ({args.short_video_threshold}s) must be at least "
                f"double segment-count * segment-duration "
                f"(2 * {args.segment_count} * {args.segment_duration}s = {min_threshold}s)"
            )

    if args.audio_bitrate < 0:
        errors.append("--audio-bitrate must be >= 0 (use 0 for auto)")

    if args.directory is not None:
        if not args.directory.is_dir():
            errors.append(f"directory does not exist or is not a directory: {args.directory}")

    if args.scratch_dir is not None and args.scratch_dir.exists() and not args.scratch_dir.is_dir():
        errors.append(f"--scratch-dir exists but is not a directory: {args.scratch_dir}")

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

    Returns one of:
      ``"processed"``, ``"failed"``,
      ``"skipped:no_info"``, ``"skipped:low_bitrate"``,
      ``"skipped:already_av1"``, ``"skipped:output_exists"``,
      ``"skipped:dry_run"``,
      ``"processed:crf_ceiling"`` (encoded with fallback CRF because search exceeded max-crf),
      ``"processed:crf_ceiling:low_original"`` (as above + original bitrate was low).
    """
    info = get_video_info(input_path)
    if info is None:
        log.warning("  Could not read video info, skipping")
        return "skipped:no_info"

    if info.width > info.height:
        orientation = "landscape"
    elif info.height > info.width:
        orientation = "portrait"
    else:
        orientation = "square"
    total_secs = int(round(info.duration_sec))
    duration_hms = f"{total_secs // 3600:02d}:{total_secs % 3600 // 60:02d}:{total_secs % 60:02d}"
    log.info(
        "  Codec=%s Resolution=%dx%d (%s) FPS=%.3f Bitrate=%d kbps "
        "AudioCh=%d Duration=%s (%.1fs) Frames=%d",
        info.codec, info.width, info.height, orientation, info.fps,
        info.bitrate_kbps, info.audio_channels, duration_hms,
        info.duration_sec, info.frame_count,
    )

    # Skip if source bitrate is already at or below the skip threshold
    if args.skip_below_bitrate > 0 and 0 < info.bitrate_kbps <= args.skip_below_bitrate:
        log.info(
            "  Source bitrate (%d kbps) is at or below --skip-below-bitrate (%d kbps), skipping",
            info.bitrate_kbps, args.skip_below_bitrate,
        )
        return "skipped:low_bitrate"

    # Skip AV1 / VP9
    if info.codec in SKIP_CODECS:
        log.info("  Already encoded as %s, skipping", info.codec)
        return "skipped:already_av1"

    # Check output
    output_path = get_output_path(input_path)
    if output_path.exists():
        log.info("  Output already exists: %s, skipping", output_path)
        return "skipped:output_exists"

    # Log VFR / non-30fps
    if info.is_vfr:
        log.warning("  Variable frame rate detected")
    if info.fps > 0 and abs(info.fps - 30.0) > 1.0:
        log.info("  Video is %.3f fps (not 30 fps)", info.fps)

    # Audio bitrate
    has_audio = info.audio_channels > 0 and info.audio_codec is not None
    audio_str, audio_kbps = compute_audio_bitrate(info, args.audio_bitrate)
    if has_audio:
        log.info("  Audio: %d channel(s) -> Opus %s", info.audio_channels, audio_str)
    else:
        log.info("  No audio stream detected")

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
        args.accept_window,
        args.confident_window,
        args.sample_window_buffer,
    )

    extra_args = build_extra_args(info)

    if args.dry_run:
        log.info("  [DRY RUN] Would encode to: %s", output_path)
        return "skipped:dry_run"

    # Determine if short video — videos at or under the threshold are encoded in
    # full during the search (no segment sampling); longer videos use the short
    # multi-segment encoding unless --precise mode is active.
    is_short_video = info.duration_sec > 0 and info.duration_sec <= args.short_video_threshold

    if is_short_video:
        log.info(
            "  Video (%.1fs) <= %.0fs threshold, encoding full video during search",
            info.duration_sec, args.short_video_threshold,
        )

    # Choose search method (smart search is the default)
    search_method = {
        "smart": smart_search_next,
        "interpolation": interpolation_next,
        "binary": binary_search_next,
    }[args.search_method]
    search_label = f" ({args.search_method})"

    # Compute segment offsets for multi-segment sampling
    offsets: list[float] | None = None
    if not is_short_video:
        offsets = compute_segment_offsets(
            info.duration_sec, args.segment_count, args.segment_duration,
        )
        log.debug("  Segment offsets: %s", offsets)

    log.info("  Starting CRF search%s...", search_label)

    # Track temp files for cleanup on exceptions
    temp_files: list[Path] = []

    try:
        # Stage onto local scratch when --scratch-dir is set, so the many decode
        # passes read from local disk instead of the source location (e.g. a NAS
        # share). Only stage now that we've committed to encoding — files skipped
        # above are never copied. src/output_path are redirected to the local
        # copies and registered in temp_files so the finally block removes them on
        # every exit path; dest_output keeps the real destination for the final
        # move performed by _finalize_output() on success.
        dest_output = output_path
        src = input_path
        if args.scratch_dir is not None:
            args.scratch_dir.mkdir(parents=True, exist_ok=True)
            token = uuid.uuid4().hex
            src = args.scratch_dir / f"{token}-in{input_path.suffix}"
            output_path = args.scratch_dir / f"{token}-out{dest_output.suffix}"
            temp_files.append(src)
            temp_files.append(output_path)
            log.info("  Caching source locally: %s", src)
            shutil.copy2(input_path, src)

        def _finalize_output() -> None:
            """Move the locally-encoded output to its destination when caching."""
            if output_path != dest_output and output_path.exists():
                log.info("  Writing final output to %s", dest_output)
                shutil.move(str(output_path), str(dest_output))

        result = find_optimal_crf(
            src, windows, extra_args, audio_str,
            args.preset, audio_kbps, args.max_iterations,
            args.crf_min, args.crf_max, args.crf_ceiling_fallback,
            search_method=search_method,
            offsets=offsets, seg_duration=args.segment_duration,
            full_encode=is_short_video,
            has_audio=has_audio,
        )
        if result.temp_file:
            temp_files.append(result.temp_file)

        # ── max-crf ceiling check ────────────────────────────────────
        crf_ceiling_triggered: bool | str = False
        if result.crf_ceiling_used:
            log.warning(
                "  Search reached --crf-max (%d) with bitrate still above target. "
                "Encoding with --crf-ceiling-fallback=%d.",
                args.crf_max, args.crf_ceiling_fallback,
            )
            crf_ceiling_triggered = True
            if info.bitrate_kbps > 0 and info.bitrate_kbps < 2 * args.target_bitrate:
                log.warning(
                    "  Original bitrate (%d kbps) is less than double the target (%d kbps). "
                    "The source may already be low-quality.",
                    info.bitrate_kbps, args.target_bitrate,
                )
                crf_ceiling_triggered = "low_original"

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
                src, output_path, result.crf, extra_args,
                audio_str, args.preset, info.duration_sec, has_audio=has_audio,
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
            _finalize_output()
            if crf_ceiling_triggered == "low_original":
                return "processed:crf_ceiling:low_original"
            if crf_ceiling_triggered:
                return "processed:crf_ceiling"
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
        if args.precise and out_of_range and not is_short_video and not crf_ceiling_triggered:
            log.info("  --precise: final bitrate out of range, starting full-video search...")
            # Preserve the full encode we just made at result.crf as a reusable
            # seed instead of deleting it: if the precise search settles on that
            # CRF, this file becomes the output with no re-encode. Moved out of
            # output_path (same directory, so it's a cheap rename) so output_path
            # starts empty for the search's chosen winner.
            seed_temp = output_path.with_name(
                f"{output_path.stem}.precise-seed{output_path.suffix}"
            )
            output_path.replace(seed_temp)
            temp_files.append(seed_temp)

            precise_result = find_optimal_crf(
                src, windows, extra_args, audio_str,
                args.preset, audio_kbps, args.max_iterations,
                args.crf_min, args.crf_max, args.crf_ceiling_fallback,
                search_method=search_method,
                full_encode=True,
                seed_crf=result.crf,
                seed_known=[CrfPoint(result.crf, out_bitrate)],
                seed_temp_files={result.crf: seed_temp},
                has_audio=has_audio,
            )
            if precise_result.temp_file:
                temp_files.append(precise_result.temp_file)

            if precise_result.crf_ceiling_used:
                log.warning(
                    "  Precise search also reached --crf-max; encoding with "
                    "--crf-ceiling-fallback=%d",
                    args.crf_ceiling_fallback,
                )
                exit_code = encode_full(
                    src, output_path, args.crf_ceiling_fallback, extra_args,
                    audio_str, args.preset, info.duration_sec, has_audio=has_audio,
                )
                if exit_code != 0 or not output_path.exists():
                    log.error("  FAILED: precise crf-ceiling encode did not produce output")
                    return "failed"
                crf_ceiling_triggered = True
                if info.bitrate_kbps > 0 and info.bitrate_kbps < 2 * args.target_bitrate:
                    crf_ceiling_triggered = "low_original"
            elif precise_result.temp_file and precise_result.temp_file.exists():
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

        _finalize_output()
        if crf_ceiling_triggered == "low_original":
            return "processed:crf_ceiling:low_original"
        if crf_ceiling_triggered:
            return "processed:crf_ceiling"
        return "processed"

    finally:
        # Clean up any temp files that weren't moved to the output path
        for tmp in temp_files:
            try:
                if tmp.exists():
                    tmp.unlink(missing_ok=True)
            except OSError:
                pass


def _print_statistics(
    total: int,
    already_reencoded: int,
    processed: int,
    failed: int,
    skipped_no_info: int,
    skipped_low_bitrate: int,
    skipped_already_av1: int,
    skipped_dry_run: int,
    crf_ceiling_count: int,
    low_original_videos: list[Path],
    interrupted: bool = False,
) -> None:
    """Print a summary of the encoding session statistics."""
    log.info("=" * 60)
    log.info("Session %s", "Interrupted — Partial Statistics" if interrupted else "Complete — Statistics")
    log.info("=" * 60)
    log.info("Total video files found : %d", total)
    log.info("")
    log.info("Already re-encoded      : %d  (output file already existed, not counted in skips below)", already_reencoded)
    log.info("")
    log.info("Skipped (excl. above)   : %d", skipped_no_info + skipped_low_bitrate + skipped_already_av1 + skipped_dry_run)
    if skipped_no_info:
        log.info("  ├─ Could not read video info : %d", skipped_no_info)
    if skipped_already_av1:
        log.info("  ├─ Already AV1/VP9           : %d", skipped_already_av1)
    if skipped_low_bitrate:
        log.info("  ├─ Bitrate at/below min      : %d", skipped_low_bitrate)
    if skipped_dry_run:
        log.info("  └─ Dry run                   : %d", skipped_dry_run)
    log.info("")
    log.info("Successfully re-encoded : %d", processed)
    log.info("Failed                  : %d", failed)
    log.info("")
    log.info("Exceeded max-CRF (used --crf-ceiling-fallback) : %d", crf_ceiling_count)
    if crf_ceiling_count:
        log.info("  └─ Of which had low original bitrate (<2x target) : %d", len(low_original_videos))
        if low_original_videos:
            log.info("")
            log.info("  Low-bitrate originals that triggered CRF ceiling:")
            for p in low_original_videos:
                log.info("    %s", p)
    log.info("=" * 60)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(args)
    setup_logging(args.log_file, args.verbose)

    # Check for ffmpeg/ffprobe
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        log.error("ffmpeg and ffprobe must be on PATH")
        sys.exit(1)

    cwd = args.directory.resolve() if args.directory else Path.cwd()
    log.info("=" * 60)
    log.info("AV1 Re-encode Session Started")
    log.info("=" * 60)
    log.info("Target: %d kbps", args.target_bitrate)
    log.info(
        "Skip below bitrate: %s",
        f"{args.skip_below_bitrate} kbps" if args.skip_below_bitrate > 0 else "disabled (0)",
    )
    log.info("Accept window: %d kbps", args.accept_window)
    log.info("Confident window: %d kbps", args.confident_window)
    log.info("Sample buffer: %d kbps", args.sample_window_buffer)
    log.info("Min CRF: %d | Max CRF: %d | CRF ceiling fallback: %d", args.crf_min, args.crf_max, args.crf_ceiling_fallback)
    if args.scratch_dir is not None:
        log.info("Scratch dir: %s (sources staged locally before encoding)", args.scratch_dir)

    windows = compute_windows(
        args.target_bitrate,
        args.accept_window,
        args.confident_window,
        args.sample_window_buffer,
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

    # Statistics counters
    processed = 0
    failed = 0
    already_reencoded = 0
    skipped_no_info = 0
    skipped_low_bitrate = 0
    skipped_already_av1 = 0
    skipped_dry_run = 0
    crf_ceiling_count = 0
    low_original_videos: list[Path] = []

    interrupted = False

    try:
        for i, file_path in enumerate(video_files, 1):
            log.info("-" * 60)
            log.info("Processing [%d/%d]: %s", i, total, file_path)

            result = process_file(file_path, args)

            if result == "processed":
                processed += 1
            elif result == "processed:crf_ceiling":
                processed += 1
                crf_ceiling_count += 1
            elif result == "processed:crf_ceiling:low_original":
                processed += 1
                crf_ceiling_count += 1
                low_original_videos.append(file_path)
            elif result == "failed":
                failed += 1
            elif result == "skipped:output_exists":
                already_reencoded += 1
            elif result == "skipped:no_info":
                skipped_no_info += 1
            elif result == "skipped:low_bitrate":
                skipped_low_bitrate += 1
            elif result == "skipped:already_av1":
                skipped_already_av1 += 1
            elif result == "skipped:dry_run":
                skipped_dry_run += 1

    except KeyboardInterrupt:
        interrupted = True
        log.warning("Interrupted by user (Ctrl+C)")

    _print_statistics(
        total=total,
        already_reencoded=already_reencoded,
        processed=processed,
        failed=failed,
        skipped_no_info=skipped_no_info,
        skipped_low_bitrate=skipped_low_bitrate,
        skipped_already_av1=skipped_already_av1,
        skipped_dry_run=skipped_dry_run,
        crf_ceiling_count=crf_ceiling_count,
        low_original_videos=low_original_videos,
        interrupted=interrupted,
    )


if __name__ == "__main__":
    main()
