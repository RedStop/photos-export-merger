#Requires -Version 5.1
<#
.SYNOPSIS
    Batch re-encode videos to AV1 (libsvtav1) with automatic CRF tuning.
.DESCRIPTION
    Recursively finds videos in the current directory and subdirectories,
    determines the optimal CRF value via binary search on a short sample
    to achieve a bitrate just below 2500 kbit/s (or below the original bitrate
    if that is already under 2500), then encodes the full video. Videos shorter
    than the sample duration are handled efficiently by reusing the best
    binary-search encode as the final output.
.NOTES
    Requires ffmpeg and ffprobe on PATH.
#>

[CmdletBinding()]
param(
    [int]$TargetBitrateKbps = 2500,
    [int]$MarginKbps = 500,           # acceptable undershoot (target - margin .. target)
    [int]$CrfMin = 1,
    [int]$CrfMax = 63,
    [int]$MaxIterations = 15,
    [string]$SampleDuration = "10",   # seconds of sample to encode
    [int]$AudioBitrateKbps = 0,        # opus bitrate; 0 = auto (64 kbps/channel)
    [switch]$DryRun,                   # if set, only log what would be done
    [switch]$Help                      # show usage help
)

# ── Help ─────────────────────────────────────────────────────────────────────
if ($Help) {
    Write-Host @"

  reencode-av1.ps1 — Batch re-encode videos to AV1 with automatic CRF tuning

  USAGE
    .\reencode-av1.ps1 [options]

  OPTIONS
    -TargetBitrateKbps <int>   Target video bitrate in kbit/s (default: 2500)
    -MarginKbps <int>          Acceptable undershoot below target (default: 500)
                               Result will be in range [target-margin, target]
    -CrfMin <int>              Minimum CRF to try (default: 1)
    -CrfMax <int>              Maximum CRF to try (default: 63)
    -MaxIterations <int>       Max binary search iterations (default: 15)
    -SampleDuration <string>   Seconds of video to sample for CRF search (default: 10)
    -AudioBitrateKbps <int>    Opus audio bitrate in kbit/s (default: auto)
                               Auto = 64 kbps per channel (e.g. 128 for stereo, 384 for 5.1)
    -DryRun                    Show what would be done without encoding
    -Help                      Show this help message

  BEHAVIOR
    - Recursively scans the current directory for video files
    - Skips videos already encoded as AV1 or VP9
    - Binary-searches CRF values by encoding a short sample to find one
      that produces a bitrate in [target-margin, target] kbit/s
    - Videos shorter than SampleDuration are handled efficiently: the
      binary search encodes the full video and the best result is reused
      as the final output (no redundant re-encode)
    - If the original bitrate is already below the target, targets the
      original bitrate instead (never increases bitrate)
    - Downscales videos above 1080p (landscape: 1920:-2, portrait: -2:1080)
      but never upscales
    - Adjusts GOP/keyint to ~8s/~4s based on actual FPS
    - Logs VFR and non-30fps videos
    - Output is .mkv; original .mkv files get a "-reencoded" suffix
    - Auto-selects Opus audio bitrate at 64 kbps per channel (128k stereo,
      384k for 5.1) unless overridden with -AudioBitrateKbps
    - Logs everything to reencode-av1.log in the script directory
    - Shows real-time encoding progress (speed, time, fps, bitrate, file size)

  EXAMPLES
    .\reencode-av1.ps1                              # Default settings
    .\reencode-av1.ps1 -TargetBitrateKbps 2000      # Lower target
    .\reencode-av1.ps1 -DryRun                      # Preview only
    .\reencode-av1.ps1 -SampleDuration 20           # Longer sample for accuracy
    .\reencode-av1.ps1 -AudioBitrateKbps 192        # Fixed 192k Opus audio

"@
    exit 0
}

# ── Globals ──────────────────────────────────────────────────────────────────
$VideoExtensions = @('.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.ts', '.mpg', '.mpeg', '.3gp')
$LogFile = Join-Path $PSScriptRoot "reencode-av1.log"
$ErrorActionPreference = 'Continue'

# ── Helpers ──────────────────────────────────────────────────────────────────

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$timestamp] [$Level] $Message"
    Write-Host $line
    Add-Content -Path $LogFile -Value $line
}

function Format-FfmpegCommand {
    <#
    .SYNOPSIS
        Formats an ffmpeg argument array into a readable command string for logging.
    #>
    param([string[]]$Arguments)

    $parts = @('ffmpeg')
    foreach ($arg in $Arguments) {
        if ($arg -match '\s') {
            $parts += "`"$arg`""
        } else {
            $parts += $arg
        }
    }
    return $parts -join ' '
}

function Invoke-FfmpegWithProgress {
    <#
    .SYNOPSIS
        Runs ffmpeg and parses stderr for progress info, displaying a live status line.
    #>
    param(
        [string[]]$FfArgs,
        [double]$TotalDurationSec = 0
    )

    $psi = [System.Diagnostics.ProcessStartInfo]::new()
    $psi.FileName = 'ffmpeg'
    $psi.Arguments = ($FfArgs | ForEach-Object {
        if ($_ -match '\s') { "`"$_`"" } else { $_ }
    }) -join ' '
    $psi.UseShellExecute = $false
    $psi.RedirectStandardError = $true
    $psi.RedirectStandardOutput = $true
    $psi.CreateNoWindow = $true

    $proc = [System.Diagnostics.Process]::Start($psi)
    $lastProgressLen = 0

    while (-not $proc.StandardError.EndOfStream) {
        $line = $proc.StandardError.ReadLine()
        if (-not $line) { continue }

        # ffmpeg progress lines look like:
        # frame=  123 fps= 12 q=35.0 size=    1234kB time=00:00:04.10 bitrate= 2465.3kbits/s speed=0.401x
        if ($line -match 'frame=\s*(\d+)' -and $line -match 'time=(\S+)') {
            $frame   = $Matches[1]
            $null    = $line -match 'time=(\S+)';      $timeStr  = $Matches[1]
            $null    = $line -match 'speed=(\S+)';     $speed    = if ($Matches[1]) { $Matches[1] } else { '?' }
            $null    = $line -match 'fps=\s*(\S+)';    $encFps   = if ($Matches[1]) { $Matches[1] } else { '?' }
            $null    = $line -match 'bitrate=\s*(\S+)'; $bitrate = if ($Matches[1]) { $Matches[1] } else { '?' }
            $null    = $line -match 'size=\s*(\S+)';   $size     = if ($Matches[1]) { $Matches[1] } else { '?' }

            # Parse time=HH:MM:SS.xx into seconds for percentage
            $pctStr = ""
            if ($TotalDurationSec -gt 0) {
                $timeParts = $timeStr -split '[:.]'
                if ($timeParts.Count -ge 3) {
                    $currentSec = [double]$timeParts[0] * 3600 + [double]$timeParts[1] * 60 + [double]$timeParts[2]
                    if ($timeParts.Count -ge 4) {
                        $currentSec += [double]"0.$($timeParts[3])"
                    }
                    $pct = [math]::Min(100, [math]::Round($currentSec / $TotalDurationSec * 100, 1))
                    $pctStr = " ${pct}%"
                }
            }

            $status = "  [ENCODE]${pctStr} time=${timeStr} frame=${frame} fps=${encFps} bitrate=${bitrate} size=${size} speed=${speed}"

            # Pad to overwrite previous line fully
            $padded = $status.PadRight($lastProgressLen)
            Write-Host "`r$padded" -NoNewline
            $lastProgressLen = $status.Length
        }
    }

    $proc.WaitForExit()

    # Clear the progress line
    if ($lastProgressLen -gt 0) {
        Write-Host "`r$(' ' * $lastProgressLen)" -NoNewline
        Write-Host "`r" -NoNewline
    }

    return $proc.ExitCode
}

function Get-VideoInfo {
    <#
    .SYNOPSIS
        Returns a PSObject with codec, bitrate, width, height, fps, isVFR, channels info.
    #>
    param([string]$FilePath)

    # Get video stream info
    $probeJson = & ffprobe -v quiet -print_format json `
        -show_streams -show_format "$FilePath" 2>&1

    $probe = $probeJson | Out-String | ConvertFrom-Json

    $videoStream = $probe.streams | Where-Object { $_.codec_type -eq 'video' } | Select-Object -First 1
    $audioStream = $probe.streams | Where-Object { $_.codec_type -eq 'audio' } | Select-Object -First 1

    if (-not $videoStream) {
        return $null
    }

    # Parse frame rate from avg_frame_rate (e.g. "30000/1001")
    $fps = 0.0
    if ($videoStream.avg_frame_rate -and $videoStream.avg_frame_rate -ne '0/0') {
        $parts = $videoStream.avg_frame_rate -split '/'
        if ($parts.Count -eq 2 -and [double]$parts[1] -ne 0) {
            $fps = [math]::Round([double]$parts[0] / [double]$parts[1], 3)
        }
    }

    # Detect VFR: compare avg_frame_rate vs r_frame_rate
    $isVFR = $false
    if ($videoStream.r_frame_rate -and $videoStream.avg_frame_rate) {
        $rParts = $videoStream.r_frame_rate -split '/'
        $rFps = 0.0
        if ($rParts.Count -eq 2 -and [double]$rParts[1] -ne 0) {
            $rFps = [math]::Round([double]$rParts[0] / [double]$rParts[1], 3)
        }
        if ($rFps -gt 0 -and $fps -gt 0 -and [math]::Abs($rFps - $fps) -gt 0.5) {
            $isVFR = $true
        }
    }

    # Bitrate: prefer stream bitrate, fall back to format bitrate minus audio estimate
    $bitrateKbps = 0
    if ($videoStream.bit_rate) {
        $bitrateKbps = [math]::Round([double]$videoStream.bit_rate / 1000, 0)
    } elseif ($probe.format.bit_rate) {
        $totalBitrate = [double]$probe.format.bit_rate / 1000
        $audioBitrate = 0
        if ($audioStream -and $audioStream.bit_rate) {
            $audioBitrate = [double]$audioStream.bit_rate / 1000
        } elseif ($audioStream) {
            $audioBitrate = 128  # estimate
        }
        $bitrateKbps = [math]::Round($totalBitrate - $audioBitrate, 0)
    }

    $audioChannels = 2
    if ($audioStream -and $audioStream.channels) {
        $audioChannels = [int]$audioStream.channels
    }

    $width  = [int]$videoStream.width
    $height = [int]$videoStream.height

    $durationSec = 0.0
    if ($probe.format.duration) {
        $durationSec = [double]$probe.format.duration
    }

    return [PSCustomObject]@{
        Codec         = $videoStream.codec_name
        Width         = $width
        Height        = $height
        Fps           = $fps
        IsVFR         = $isVFR
        BitrateKbps   = $bitrateKbps
        DurationSec   = $durationSec
        AudioChannels = $audioChannels
        AudioCodec    = if ($audioStream) { $audioStream.codec_name } else { $null }
    }
}

function Get-SampleBitrate {
    <#
    .SYNOPSIS
        Encodes the first N seconds at the given CRF and returns the video bitrate in kbit/s.
        When KeepTempFile is set, the temp file path is returned as a second value instead of being deleted.
    #>
    param(
        [string]$InputFile,
        [int]$Crf,
        [string[]]$ExtraArgs,
        [string]$Duration = "10",
        [string]$AudioBitrate = "128k",
        [switch]$KeepTempFile
    )

    $tempFile = Join-Path ([System.IO.Path]::GetTempPath()) "av1_sample_$([guid]::NewGuid().ToString('N')).mkv"
    $deleteTempFile = -not $KeepTempFile

    try {
        $ffArgs = @(
            '-y', '-hide_banner', '-loglevel', 'error',
            '-t', $Duration,
            '-i', $InputFile
        ) + $ExtraArgs + @(
            '-c:v', 'libsvtav1',
            '-preset', '3',
            '-crf', $Crf.ToString(),
            '-pix_fmt', 'yuv420p10le',
            '-c:a', 'libopus', '-b:a', $AudioBitrate, '-vbr', 'on', '-compression_level', '10',
            $tempFile
        )

        & ffmpeg @ffArgs 2>&1 | Out-Null

        if (-not (Test-Path $tempFile)) {
            if ($KeepTempFile) { return @{ Bitrate = -1; TempFile = $null } }
            return -1
        }

        # Get the resulting video bitrate
        $probeJson = & ffprobe -v quiet -print_format json -show_streams -show_format "$tempFile" 2>&1
        $probe = $probeJson | Out-String | ConvertFrom-Json

        $vidStream = $probe.streams | Where-Object { $_.codec_type -eq 'video' } | Select-Object -First 1
        $bitrate = -1
        if ($vidStream -and $vidStream.bit_rate) {
            $bitrate = [math]::Round([double]$vidStream.bit_rate / 1000, 0)
        } else {
            # Fallback: compute from file size and duration
            $fileSize = (Get-Item $tempFile).Length  # bytes
            $dur = [double]$probe.format.duration
            if ($dur -gt 0) {
                $abKbps = [int]($AudioBitrate -replace '[^0-9]','')
                $audioBits = $abKbps * 1000 * $dur  # approximate audio bits
                $videoBits = ($fileSize * 8) - $audioBits
                $bitrate = [math]::Round($videoBits / $dur / 1000, 0)
            }
        }

        if ($KeepTempFile -and $bitrate -ge 0) {
            $deleteTempFile = $false
            return @{ Bitrate = $bitrate; TempFile = $tempFile }
        } elseif ($KeepTempFile) {
            return @{ Bitrate = -1; TempFile = $null }
        }
        return $bitrate
    } finally {
        if ($deleteTempFile -and (Test-Path $tempFile)) {
            Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
        }
    }
}

function Find-OptimalCrf {
    <#
    .SYNOPSIS
        Binary-search for a CRF that yields a bitrate in [targetBitrate - margin, targetBitrate].
        When IsFullEncode is set, the best sample file is kept and returned for reuse as the final output.
    #>
    param(
        [string]$InputFile,
        [int]$EffectiveTargetKbps,
        [string[]]$ExtraArgs,
        [string]$Duration = "10",
        [string]$AudioBitrate = "128k",
        [switch]$IsFullEncode
    )

    $lo = $CrfMin
    $hi = $CrfMax
    $bestCrf = -1
    $bestBitrate = 0
    $bestTempFile = $null
    $iteration = 0
    $lowerBound = $EffectiveTargetKbps - $MarginKbps

    Write-Log "  Binary search: target=${EffectiveTargetKbps} kbps, acceptable range=[${lowerBound}, ${EffectiveTargetKbps}]"

    while ($lo -le $hi -and $iteration -lt $MaxIterations) {
        $iteration++
        $mid = [math]::Floor(($lo + $hi) / 2)

        if ($IsFullEncode) {
            $sampleResult = Get-SampleBitrate -InputFile $InputFile -Crf $mid -ExtraArgs $ExtraArgs -Duration $Duration -AudioBitrate $AudioBitrate -KeepTempFile
            $bitrate = $sampleResult.Bitrate
            $tempFile = $sampleResult.TempFile
        } else {
            $bitrate = Get-SampleBitrate -InputFile $InputFile -Crf $mid -ExtraArgs $ExtraArgs -Duration $Duration -AudioBitrate $AudioBitrate
            $tempFile = $null
        }

        if ($bitrate -lt 0) {
            Write-Log "  Iteration ${iteration}: CRF=$mid -> encode failed" "WARN"
            if ($tempFile -and (Test-Path $tempFile)) { Remove-Item $tempFile -Force -ErrorAction SilentlyContinue }
            $lo = $mid + 1
            continue
        }

        Write-Log "  Iteration ${iteration}: CRF=$mid -> ${bitrate} kbps"

        $isNewBest = $false
        if ($bitrate -le $EffectiveTargetKbps -and $bitrate -ge $lowerBound) {
            # In the sweet spot
            $isNewBest = $true
            $bestCrf = $mid
            $bestBitrate = $bitrate
            # Try to get a slightly lower CRF (better quality) that's still in range
            $hi = $mid - 1
        } elseif ($bitrate -gt $EffectiveTargetKbps) {
            # Bitrate too high, increase CRF
            $lo = $mid + 1
        } else {
            # Bitrate too low (below acceptable range), decrease CRF for more quality
            if ($bestCrf -lt 0 -or $mid -lt $bestCrf) {
                # Keep this as a fallback - it's below target which is fine, just not optimal
                $isNewBest = $true
                $bestCrf = $mid
                $bestBitrate = $bitrate
            }
            $hi = $mid - 1
        }

        # Manage temp files when encoding the full video during search
        if ($IsFullEncode -and $tempFile) {
            if ($isNewBest) {
                # Clean up previous best temp file
                if ($bestTempFile -and (Test-Path $bestTempFile)) {
                    Remove-Item $bestTempFile -Force -ErrorAction SilentlyContinue
                }
                $bestTempFile = $tempFile
            } else {
                # Not the best, clean up
                Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
            }
        }
    }

    # If we never found anything in range, but we have a best that's below target, use it
    if ($bestCrf -lt 0) {
        # Last resort: just use CRF 37
        Write-Log "  Binary search did not converge, falling back to CRF 37" "WARN"
        $bestCrf = 37
        $bestBitrate = 0
        if ($bestTempFile -and (Test-Path $bestTempFile)) {
            Remove-Item $bestTempFile -Force -ErrorAction SilentlyContinue
            $bestTempFile = $null
        }
    }

    Write-Log "  Selected CRF=$bestCrf (estimated ${bestBitrate} kbps)"
    return @{ Crf = $bestCrf; EstimatedBitrate = $bestBitrate; TempFile = $bestTempFile }
}

function Get-OutputPath {
    param([string]$InputFile)

    $dir  = [System.IO.Path]::GetDirectoryName($InputFile)
    $name = [System.IO.Path]::GetFileNameWithoutExtension($InputFile)
    $ext  = [System.IO.Path]::GetExtension($InputFile).ToLower()

    if ($ext -eq '.mkv') {
        return Join-Path $dir "${name}-reencoded.mkv"
    } else {
        return Join-Path $dir "${name}.mkv"
    }
}

function Build-ExtraArgs {
    <#
    .SYNOPSIS
        Returns extra ffmpeg arguments for scaling, GOP, and fps-related settings.
    #>
    param([PSCustomObject]$Info)

    $args = [System.Collections.Generic.List[string]]::new()

    # ── Scaling ──────────────────────────────────────────────────────────
    $needScale = $false
    $scaleFilter = ""

    if ($Info.Width -gt $Info.Height) {
        # Landscape
        if ($Info.Height -gt 1080) {
            $needScale = $true
            $scaleFilter = "scale=-2:1080"
        } elseif ($Info.Width -gt 1920) {
            $needScale = $true
            $scaleFilter = "scale=1920:-2"
        }
    } else {
        # Portrait or square
        if ($Info.Width -gt 1080) {
            $needScale = $true
            $scaleFilter = "scale=1080:-2"
        } elseif ($Info.Height -gt 1920) {
            $needScale = $true
            $scaleFilter = "scale=-2:1920"
        }
    }

    if ($needScale) {
        $args.Add('-vf')
        $args.Add($scaleFilter)
        Write-Log "  Downscaling with: $scaleFilter"
    }

    # ── GOP / keyframe interval (~8 seconds) ─────────────────────────────
    $fps = $Info.Fps
    if ($fps -le 0) { $fps = 30.0 }

    $gopSize   = [math]::Round($fps * 8, 0)          # ~8 seconds
    $keyintMin = [math]::Round($fps * 4, 0)           # ~4 seconds (half of GOP)

    $args.Add('-g')
    $args.Add($gopSize.ToString())
    $args.Add('-keyint_min')
    $args.Add($keyintMin.ToString())

    return $args.ToArray()
}

# ── Main ─────────────────────────────────────────────────────────────────────

Write-Log "========== AV1 Re-encode Session Started =========="
Write-Log "Target bitrate: ${TargetBitrateKbps} kbps, margin: ${MarginKbps} kbps"
Write-Log "Scanning for video files in: $(Get-Location)"

$videoFiles = Get-ChildItem -Path . -Recurse -File |
    Where-Object { $VideoExtensions -contains $_.Extension.ToLower() }

$totalFiles  = $videoFiles.Count
$processed   = 0
$skipped     = 0
$failed      = 0

Write-Log "Found $totalFiles video file(s)"

foreach ($file in $videoFiles) {
    $inputPath = $file.FullName
    Write-Log "────────────────────────────────────────────────────────"
    Write-Log "Processing [$($processed + $skipped + $failed + 1)/$totalFiles]: $inputPath"

    # ── Get video info ───────────────────────────────────────────────
    $info = Get-VideoInfo -FilePath $inputPath

    if (-not $info) {
        Write-Log "  Could not read video info, skipping" "WARN"
        $skipped++
        continue
    }

    Write-Log "  Codec=$($info.Codec) Resolution=$($info.Width)x$($info.Height) FPS=$($info.Fps) Bitrate=$($info.BitrateKbps) kbps AudioCh=$($info.AudioChannels)"

    # ── Skip AV1 / VP9 ───────────────────────────────────────────────
    $skipCodecs = @('av1', 'vp9')
    if ($skipCodecs -contains $info.Codec) {
        Write-Log "  Already encoded as $($info.Codec), skipping"
        $skipped++
        continue
    }

    # ── Check output already exists ──────────────────────────────────
    $outputPath = Get-OutputPath -InputFile $inputPath
    if (Test-Path $outputPath) {
        Write-Log "  Output already exists: $outputPath, skipping"
        $skipped++
        continue
    }

    # ── Log VFR / non-30fps ──────────────────────────────────────────
    if ($info.IsVFR) {
        Write-Log "  WARNING: Variable frame rate detected" "WARN"
    }
    if ($info.Fps -gt 0 -and ([math]::Abs($info.Fps - 30.0) -gt 1.0)) {
        Write-Log "  NOTE: Video is $($info.Fps) fps (not 30 fps)" "INFO"
    }

    # ── Determine effective audio bitrate ────────────────────────────
    if ($AudioBitrateKbps -gt 0) {
        $effectiveAudioBitrate = "${AudioBitrateKbps}k"
    } else {
        $autoAudioKbps = $info.AudioChannels * 64
        $effectiveAudioBitrate = "${autoAudioKbps}k"
    }
    Write-Log "  Audio: $($info.AudioChannels) channel(s) -> Opus ${effectiveAudioBitrate}bps"

    # ── Determine effective target bitrate ────────────────────────────
    $effectiveTarget = $TargetBitrateKbps
    if ($info.BitrateKbps -gt 0 -and $info.BitrateKbps -lt $TargetBitrateKbps) {
        $effectiveTarget = $info.BitrateKbps
        Write-Log "  Original bitrate ($($info.BitrateKbps) kbps) is below ${TargetBitrateKbps}, targeting original bitrate instead"
    }

    # ── Build extra ffmpeg args ──────────────────────────────────────
    $extraArgs = Build-ExtraArgs -Info $info

    if ($DryRun) {
        Write-Log "  [DRY RUN] Would encode to: $outputPath"
        $skipped++
        continue
    }

    # ── Determine effective sample duration ─────────────────────────
    $effectiveSampleDuration = $SampleDuration
    $isShortVideo = $false
    if ($info.DurationSec -gt 0 -and $info.DurationSec -le [double]$SampleDuration) {
        $isShortVideo = $true
        $effectiveSampleDuration = $info.DurationSec.ToString("F2")
        Write-Log "  Video duration ($([math]::Round($info.DurationSec, 1))s) is at or below sample duration (${SampleDuration}s), encoding full video during CRF search"
    }

    # ── Binary search for optimal CRF ────────────────────────────────
    Write-Log "  Starting CRF binary search (sampling ${effectiveSampleDuration}s)..."

    $result = Find-OptimalCrf -InputFile $inputPath `
        -EffectiveTargetKbps $effectiveTarget `
        -ExtraArgs $extraArgs `
        -Duration $effectiveSampleDuration `
        -AudioBitrate $effectiveAudioBitrate `
        -IsFullEncode:$isShortVideo

    $optimalCrf = $result.Crf

    # ── Full encode ──────────────────────────────────────────────────
    if ($isShortVideo -and $result.TempFile -and (Test-Path $result.TempFile)) {
        # Short video: reuse the best encode from the binary search
        Write-Log "  Reusing best search encode (CRF=$optimalCrf) as final output -> $outputPath"
        $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
        Move-Item -Path $result.TempFile -Destination $outputPath -Force
        $exitCode = 0
        $stopwatch.Stop()
    } else {
        # Clean up temp file if it exists but we're doing a full encode anyway (fallback CRF case)
        if ($result.TempFile -and (Test-Path $result.TempFile)) {
            Remove-Item $result.TempFile -Force -ErrorAction SilentlyContinue
        }

        Write-Log "  Full encode starting with CRF=$optimalCrf -> $outputPath"
        $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

        $ffArgs = @(
            '-y', '-hide_banner', '-stats',
            '-i', $inputPath
        ) + $extraArgs + @(
            '-c:v', 'libsvtav1',
            '-preset', '3',
            '-crf', $optimalCrf.ToString(),
            '-pix_fmt', 'yuv420p10le',
            '-c:a', 'libopus', '-b:a', $effectiveAudioBitrate, '-vbr', 'on', '-compression_level', '10',
            $outputPath
        )

        $cmdString = Format-FfmpegCommand -Arguments $ffArgs
        Write-Log "  Command: $cmdString"

        $exitCode = Invoke-FfmpegWithProgress -FfArgs $ffArgs -TotalDurationSec $info.DurationSec

        $stopwatch.Stop()
    }

    if ($exitCode -ne 0) {
        Write-Log "  FAILED: ffmpeg exited with code $exitCode" "ERROR"
        $failed++
        continue
    }

    if (Test-Path $outputPath) {
        $outInfo = Get-VideoInfo -FilePath $outputPath
        $outSizeMB = [math]::Round((Get-Item $outputPath).Length / 1MB, 1)
        $inSizeMB  = [math]::Round((Get-Item $inputPath).Length / 1MB, 1)

        if ($outInfo) {
            Write-Log "  Done in $($stopwatch.Elapsed.ToString('hh\:mm\:ss')). ${inSizeMB} MB -> ${outSizeMB} MB, output bitrate=$($outInfo.BitrateKbps) kbps"

            if ($outInfo.BitrateKbps -gt $effectiveTarget) {
                Write-Log "  WARNING: Final bitrate ($($outInfo.BitrateKbps) kbps) exceeds target (${effectiveTarget} kbps)" "WARN"
            }
        } else {
            Write-Log "  Done in $($stopwatch.Elapsed.ToString('hh\:mm\:ss')). ${inSizeMB} MB -> ${outSizeMB} MB"
        }

        $processed++
    } else {
        Write-Log "  FAILED: Output file was not created" "ERROR"
        $failed++
    }
}

Write-Log "========== Session Complete =========="
Write-Log "Processed: $processed | Skipped: $skipped | Failed: $failed | Total: $totalFiles"