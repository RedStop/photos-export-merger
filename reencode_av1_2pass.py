import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path

Q_PATTERN = re.compile(r"\bq=\s*(-?\d+(?:\.\d+)?)")


def _get_total_frames(input_path: Path) -> int | None:
    """Estimate total video frames via ffprobe (fast metadata only)."""
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=nb_frames,duration,avg_frame_rate",
             "-of", "default=noprint_wrappers=1:nokey=0", str(input_path)],
            capture_output=True, text=True, check=True,
        ).stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None
    info: dict[str, str] = {}
    for line in out.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            info[k.strip()] = v.strip()
    nb = info.get("nb_frames", "")
    if nb.isdigit() and int(nb) > 0:
        return int(nb)
    dur_s = info.get("duration", "")
    fr = info.get("avg_frame_rate", "0/0")
    try:
        num, den = fr.split("/")
        fps = float(num) / float(den) if float(den) != 0 else 0.0
        dur = float(dur_s) if dur_s else 0.0
        if fps > 0 and dur > 0:
            return int(dur * fps)
    except (ValueError, ZeroDivisionError):
        pass
    return None


def _run_pass_collecting_q(
    cmd: list[str],
    q_threshold: float | None = None,
    abort_over_count: int | None = None,
) -> tuple[int, list[float], bool]:
    """Run ffmpeg and collect q= samples from its stderr progress output.

    If abort_over_count is set, terminate the process as soon as more than
    that many frames have q > q_threshold and return aborted=True.
    """
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    q_values: list[float] = []
    over_count = 0
    aborted = False
    buf = bytearray()
    assert proc.stderr is not None
    # Read raw bytes so ffmpeg's \r progress carriage returns are preserved
    # (text mode would translate them to \n and break in-place updates).
    while True:
        b = proc.stderr.read(1)
        if not b:
            break
        if b in (b"\r", b"\n"):
            line = buf.decode("utf-8", errors="replace")
            buf.clear()
            sys.stderr.write(line + b.decode("ascii"))
            sys.stderr.flush()
            if line:
                m = Q_PATTERN.search(line)
                if m:
                    try:
                        q = float(m.group(1))
                    except ValueError:
                        q = None
                    if q is not None:
                        q_values.append(q)
                        if q_threshold is not None and q > q_threshold:
                            over_count += 1
                            if abort_over_count is not None and over_count > abort_over_count:
                                aborted = True
                                proc.terminate()
                                break
        else:
            buf.extend(b)
    if aborted:
        try:
            proc.stderr.read()
        except Exception:
            pass
    elif buf:
        sys.stderr.write(buf.decode("utf-8", errors="replace"))
        sys.stderr.flush()
    proc.wait()
    return proc.returncode, q_values, aborted


def run_ffmpeg_2pass_av1(
    input_file: str,
    output_file: str,
    target_bitrate: str = "2500k",
    encoder: str = "libsvtav1",   # or "libaom-av1"
    preset: int = 3,              # 0-13 for SVT-AV1 (higher = faster); 0-8+ for libaom
    audio_bitrate: str = "128k",
    q_threshold: float = 56.0,
    q_fraction_threshold: float = 1.0,
    fallback_crf: int = 52,
):
    """
    Perform 2-pass AV1 encoding to target a specific bitrate. After pass 1,
    inspect the q= values reported in ffmpeg's progress output. If more than
    q_fraction_threshold percent of the samples exceed q_threshold, skip pass 2
    and do a single-pass CRF encode at fallback_crf instead.
    """
    input_path = Path(input_file)
    output_path = Path(output_file)

    if not input_path.exists():
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)

    if output_path.exists():
        print(f"Error: Output file '{output_file}' already exists.")
        sys.exit(1)

    common_opts = [
        "-c:v", encoder,
        "-b:v", target_bitrate,
        "-pix_fmt", "yuv420p10le"
    ]

    # SVT-AV1 defaults to CRF (rc=0); -b:v is only honoured in VBR (rc=1).
    # The pass index and stats file are still driven by ffmpeg's own
    # -pass/-passlogfile flags (the libsvtav1 wrapper forwards them); only the
    # rate-control mode has to go through -svtav1-params.
    passlog_dir = tempfile.mkdtemp(prefix="ffmpeg2pass-")
    passlog_prefix = str(Path(passlog_dir) / "ffmpeg2pass")
    pass1_extra: list[str] = ["-pass", "1", "-passlogfile", passlog_prefix]
    pass2_extra: list[str] = ["-pass", "2", "-passlogfile", passlog_prefix]
    if encoder == "libsvtav1":
        common_opts.extend(["-preset", str(preset), "-svtav1-params", "rc=1"])
    elif encoder == "libaom-av1":
        common_opts.extend(["-cpu-used", str(preset)])
    else:
        print("Unsupported encoder.")
        sys.exit(1)

    def _cleanup_passlog() -> None:
        import shutil
        shutil.rmtree(passlog_dir, ignore_errors=True)

    total_frames = _get_total_frames(input_path)
    abort_over_count: int | None = None
    if total_frames is not None and total_frames > 0:
        abort_over_count = int(total_frames * q_fraction_threshold / 100.0)
        print(f"Total frames: {total_frames}; will abort pass 1 once "
              f"more than {abort_over_count} frames (>{q_fraction_threshold:g}%) "
              f"exceed q={q_threshold:g}.")
    else:
        print("Could not determine total frame count; pass 1 will run to completion.")

    print("Starting Pass 1 (analysis)...")
    pass1_cmd = [
        "ffmpeg", "-y", "-i", str(input_path),
        *common_opts,
        *pass1_extra,
        "-an", "-f", "null",
        "NUL" if sys.platform == "win32" else "/dev/null"
    ]

    rc, q_values, aborted = _run_pass_collecting_q(
        pass1_cmd,
        q_threshold=q_threshold,
        abort_over_count=abort_over_count,
    )
    if not aborted and rc != 0:
        print(f"Pass 1 failed with exit code {rc}.")
        _cleanup_passlog()
        sys.exit(rc)

    over_fraction: float | None = None
    if q_values:
        over_count = sum(1 for q in q_values if q > q_threshold)
        denom = total_frames if (aborted and total_frames) else len(q_values)
        over_fraction = 100.0 * over_count / denom
        avg_q = sum(q_values) / len(q_values)
        print(f"Pass 1 q samples: count={len(q_values)} avg={avg_q:.2f} "
              f"min={min(q_values):.1f} max={max(q_values):.1f} "
              f"over_{q_threshold:g}={over_count} ({over_fraction:.2f}%)")
    else:
        print("Pass 1 produced no q samples; proceeding with normal pass 2.")

    trigger_fallback = aborted or (over_fraction is not None and over_fraction > q_fraction_threshold)
    if trigger_fallback:
        if aborted:
            print(f"Aborted pass 1: >{q_fraction_threshold:g}% of frames already "
                  f"exceeded q={q_threshold}; falling back to single-pass CRF {fallback_crf}.")
        else:
            print(f"{over_fraction:.2f}% of frames exceed q={q_threshold} "
                  f"(> {q_fraction_threshold:g}%); falling back to single-pass CRF {fallback_crf}.")
        crf_cmd = [
            "ffmpeg", "-y", "-i", str(input_path),
            "-c:v", encoder,
            "-crf", str(fallback_crf),
            "-b:v", "0",
            "-pix_fmt", "yuv420p10le",
        ]
        if encoder == "libsvtav1":
            crf_cmd.extend(["-preset", str(preset)])
        elif encoder == "libaom-av1":
            crf_cmd.extend(["-cpu-used", str(preset)])
        crf_cmd.extend([
            "-c:a", "libopus",
            "-b:a", audio_bitrate,
            "-vbr", "on",
            "-compression_level", "10",
            str(output_path),
        ])
        _cleanup_passlog()
        subprocess.run(crf_cmd, check=True)
        print(f"Encoding completed successfully (CRF fallback): {output_path}")
        return

    print("Starting Pass 2 (encoding)...")
    pass2_cmd = [
        "ffmpeg", "-y", "-i", str(input_path),
        *common_opts,
        *pass2_extra,
        "-c:a", "libopus",
        "-b:a", audio_bitrate,
        "-vbr", "on",
        "-compression_level", "10",
        str(output_path)
    ]

    try:
        subprocess.run(pass2_cmd, check=True)
    finally:
        _cleanup_passlog()
    print(f"Encoding completed successfully: {output_path}")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="2-pass AV1 encode with pass-1 average-quantizer fallback to single-pass CRF."
    )
    p.add_argument("input_file", help="Path to the source video file.")
    p.add_argument("output_file", nargs="?", default=None,
                   help="Path to the output video file. Must end in .mkv or .webm. "
                        "If omitted, defaults to the input path with the extension changed to .mkv "
                        "(error if the input is already .mkv).")
    p.add_argument("--target-bitrate", default="2450k",
                   help="Target video bitrate passed to ffmpeg's -b:v for both passes "
                        "(default: 2450k).")
    p.add_argument("--encoder", default="libsvtav1", choices=["libsvtav1", "libaom-av1"],
                   help="AV1 encoder to use (default: libsvtav1). Note: --q-threshold, "
                        "--q-fraction-threshold and --fallback-crf only take effect with "
                        "libsvtav1; libaom-av1's pass 1 does not emit usable q values, so "
                        "pass 2 will always run.")
    p.add_argument("--preset", type=int, default=3,
                   help="Encoder speed/quality preset. For libsvtav1: 0-13 (higher = faster, "
                        "lower quality). For libaom-av1: maps to -cpu-used (0-8+). "
                        "Default: 3.")
    p.add_argument("--audio-bitrate", default="128k",
                   help="Audio bitrate for the libopus output stream (default: 128k).")
    p.add_argument("--q-threshold", type=float, default=56.0,
                   help="A pass-1 q value above this is considered too high (default: 56).")
    p.add_argument("--q-fraction-threshold", type=float, default=1.0,
                   help="If more than this percentage of pass-1 frames exceed --q-threshold, use the CRF fallback instead of pass 2 (default: 1.0).")
    p.add_argument("--fallback-crf", type=int, default=52,
                   help="CRF used for the single-pass fallback encode (default: 52).")
    return p


if __name__ == "__main__":
    args = _build_parser().parse_args()
    output_file = args.output_file
    if output_file is None:
        input_path = Path(args.input_file)
        if input_path.suffix.lower() == ".mkv":
            print("Error: input file is already a .mkv; please specify an output filename explicitly.")
            sys.exit(2)
        output_file = str(input_path.with_suffix(".mkv"))
    else:
        if Path(output_file).suffix.lower() not in (".mkv", ".webm"):
            print("Error: output file must have a .mkv or .webm extension.")
            sys.exit(2)
    run_ffmpeg_2pass_av1(
        input_file=args.input_file,
        output_file=output_file,
        target_bitrate=args.target_bitrate,
        encoder=args.encoder,
        preset=args.preset,
        audio_bitrate=args.audio_bitrate,
        q_threshold=args.q_threshold,
        q_fraction_threshold=args.q_fraction_threshold,
        fallback_crf=args.fallback_crf,
    )
