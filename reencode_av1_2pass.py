import subprocess
import sys
from pathlib import Path

def run_ffmpeg_2pass_av1(
    input_file: str,
    output_file: str,
    target_bitrate: str = "2500k",
    encoder: str = "libsvtav1",   # or "libaom-av1"
    preset: int = 3,              # 0-13 for SVT-AV1 (higher = faster); 0-8+ for libaom
    audio_bitrate: str = "128k",
):
    """
    Perform 2-pass AV1 encoding to target a specific bitrate.
    """
    input_path = Path(input_file)
    output_path = Path(output_file)
    
    if not input_path.exists():
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)

    # Common parameters
    common_opts = [
        "-c:v", encoder,
        "-b:v", target_bitrate,
        "-pix_fmt", "yuv420p10le"
    ]

    if encoder == "libsvtav1":
        common_opts.extend(["-preset", str(preset)])
    elif encoder == "libaom-av1":
        common_opts.extend(["-cpu-used", str(preset)])
    else:
        print("Unsupported encoder.")
        sys.exit(1)

    # Pass 1: Analysis pass (no audio, output to null)
    print("Starting Pass 1 (analysis)...")
    pass1_cmd = [
        "ffmpeg", "-y", "-i", str(input_path),
        *common_opts,
        "-pass", "1",
        "-an", "-f", "null",
        "NUL" if sys.platform == "win32" else "/dev/null"
    ]

    subprocess.run(pass1_cmd, check=True)

    # Pass 2: Encoding pass
    print("Starting Pass 2 (encoding)...")
    pass2_cmd = [
        "ffmpeg", "-y", "-i", str(input_path),
        *common_opts,
        "-pass", "2",
        "-c:a", "libopus",
        "-b:a", audio_bitrate,
        "-vbr", "on",
        "-compression_level", "10",
        str(output_path)
    ]

    subprocess.run(pass2_cmd, check=True)
    print(f"Encoding completed successfully: {output_path}")

# ========================
# Usage Example
# ========================
if __name__ == "__main__":
    run_ffmpeg_2pass_av1(
        input_file="D:\Personal\GoogleTakeout\Takeout\Google Photos\Photos from 2024\VID_20240217_125306.mp4",
        output_file="D:\Personal\GoogleTakeout\Takeout\Google Photos\Photos from 2024\VID_20240217_125306.mkv",
        target_bitrate="2450k",
    )