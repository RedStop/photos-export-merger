"""CRF search strategies: binary search and log-linear interpolation."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from pathlib import Path

from .encode import encode_sample, encode_segments
from .filters import BitrateWindows

log = logging.getLogger(__name__)


@dataclass
class CrfResult:
    """Result of a CRF search."""

    crf: int
    estimated_bitrate: int
    temp_file: Path | None = None


@dataclass
class _KnownPoint:
    """A measured CRF data point collected during interpolation search."""
    crf: int
    bitrate: int
    temp_file: Path | None = None


@dataclass
class _SearchState:
    """Mutable state shared across search phases."""

    best_crf: int = -1
    best_bitrate: int = 0
    best_temp_file: Path | None = None
    total_iterations: int = 0

    # Proven bounds: tighten the range as we learn
    proven_too_high_crf: int = -1  # lowest CRF known to overshoot target
    proven_too_low_crf: int = -1   # highest CRF known to undershoot floor

    def update_best(
        self,
        crf: int,
        bitrate: int,
        temp_file: Path | None = None,
    ) -> None:
        """Replace the current best, cleaning up the previous temp file."""
        if self.best_temp_file and self.best_temp_file.exists():
            self.best_temp_file.unlink(missing_ok=True)
        self.best_crf = crf
        self.best_bitrate = bitrate
        self.best_temp_file = temp_file

    def clear_best(self) -> None:
        """Discard the current best (and its temp file)."""
        if self.best_temp_file and self.best_temp_file.exists():
            self.best_temp_file.unlink(missing_ok=True)
        self.best_crf = -1
        self.best_bitrate = 0
        self.best_temp_file = None


def _evaluate_crf_sample(
    input_path: Path,
    crf: int,
    extra_args: list[str],
    audio_bitrate: str,
    preset: int,
    audio_bitrate_kbps: int,
    *,
    offsets: list[float] | None,
    seg_duration: float,
    full_encode: bool,
    has_audio: bool = True,
) -> tuple[int, Path | None]:
    """Evaluate a CRF value using the appropriate sampling method.

    Returns ``(bitrate_kbps, temp_file_or_none)``.
    """
    if full_encode:
        return encode_sample(
            input_path, crf, extra_args, audio_bitrate, preset,
            audio_bitrate_kbps, keep_file=True, has_audio=has_audio,
        )
    if offsets:
        bitrate = encode_segments(
            input_path, crf, extra_args, audio_bitrate, preset,
            audio_bitrate_kbps, offsets, seg_duration, has_audio=has_audio,
        )
        return bitrate, None
    # Single-segment fallback (shouldn't normally happen)
    bitrate, _ = encode_sample(
        input_path, crf, extra_args, audio_bitrate, preset,
        audio_bitrate_kbps, duration=seg_duration, has_audio=has_audio,
    )
    return bitrate, None


def _binary_search_phase(
    input_path: Path,
    lo: int,
    hi: int,
    windows: BitrateWindows,
    extra_args: list[str],
    audio_bitrate: str,
    preset: int,
    audio_bitrate_kbps: int,
    max_iterations: int,
    state: _SearchState,
    *,
    offsets: list[float] | None,
    seg_duration: float,
    full_encode: bool,
    label: str,
    has_audio: bool = True,
) -> bool:
    """Run one phase of binary search.

    Updates *state* in place.  Returns True if an acceptable result was
    found (and we can stop searching).
    """
    if full_encode:
        accept_lo = windows.final_lo
        accept_hi = windows.final_hi
        confident_lo = windows.final_accept_lo
        confident_hi = windows.final_accept_hi
    else:
        accept_lo = windows.sample_lo
        accept_hi = windows.sample_hi
        confident_lo = windows.sample_confident_lo
        confident_hi = windows.sample_confident_hi

    log.info(
        "  Binary search (%s): target=%d, accept=[%d, %d], confident=[%d, %d]",
        label, windows.target, accept_lo, accept_hi, confident_lo, confident_hi,
    )

    while lo <= hi and state.total_iterations < max_iterations:
        state.total_iterations += 1
        mid = (lo + hi) // 2

        bitrate, temp_file = _evaluate_crf_sample(
            input_path, mid, extra_args, audio_bitrate, preset,
            audio_bitrate_kbps,
            offsets=offsets, seg_duration=seg_duration,
            full_encode=full_encode, has_audio=has_audio,
        )

        if bitrate < 0:
            log.warning(
                "  Iteration %d: CRF=%d -> encode failed",
                state.total_iterations, mid,
            )
            if temp_file and temp_file.exists():
                temp_file.unlink(missing_ok=True)
            # Move away from the failing CRF
            if (mid - lo) >= (hi - mid):
                hi = mid - 1
            else:
                lo = mid + 1
            continue

        log.info(
            "  Iteration %d: CRF=%d -> %d kbps",
            state.total_iterations, mid, bitrate,
        )

        if accept_lo <= bitrate <= accept_hi:
            # In acceptable range
            state.update_best(mid, bitrate, temp_file)

            if confident_lo <= bitrate <= confident_hi:
                log.info(
                    "  Early exit: %d kbps is in confident zone [%d, %d]",
                    bitrate, confident_lo, confident_hi,
                )
                return True

            # Try lower CRF (higher quality) while staying in range
            hi = mid - 1

        elif bitrate > accept_hi:
            # Bitrate too high -> increase CRF
            if temp_file and temp_file.exists():
                temp_file.unlink(missing_ok=True)
            lo = mid + 1
            if state.proven_too_high_crf < 0 or mid < state.proven_too_high_crf:
                state.proven_too_high_crf = mid

        else:
            # Bitrate too low -> decrease CRF
            if state.best_crf < 0 or mid < state.best_crf:
                state.update_best(mid, bitrate, temp_file)
            elif temp_file and temp_file.exists():
                temp_file.unlink(missing_ok=True)
            hi = mid - 1
            if state.proven_too_low_crf < 0 or mid > state.proven_too_low_crf:
                state.proven_too_low_crf = mid

    # Check if the best is in the acceptable range
    return (
        state.best_crf >= 0
        and accept_lo <= state.best_bitrate <= accept_hi
    )


def find_optimal_crf(
    input_path: Path,
    windows: BitrateWindows,
    extra_args: list[str],
    audio_bitrate: str,
    preset: int,
    audio_bitrate_kbps: int,
    max_iterations: int,
    crf_min: int,
    crf_max: int,
    *,
    offsets: list[float] | None = None,
    seg_duration: float = 3.0,
    full_encode: bool = False,
    seed_crf: int = -1,
    seed_lo: int = -1,
    seed_hi: int = -1,
    seed_known: list[tuple[int, int]] | None = None,
    has_audio: bool = True,
) -> CrfResult:
    """Find the optimal CRF via binary search.

    Args:
        input_path: source video.
        windows: pre-computed bitrate acceptance windows.
        extra_args: additional ffmpeg arguments.
        audio_bitrate: audio bitrate string (e.g. ``"128k"``).
        preset: SVT-AV1 preset.
        audio_bitrate_kbps: numeric audio bitrate for calculations.
        max_iterations: maximum total search iterations.
        crf_min: absolute minimum CRF.
        crf_max: absolute maximum CRF.
        offsets: segment offsets for multi-segment sampling (None to disable).
        seg_duration: duration of each segment.
        full_encode: if True, encode the full video each iteration.
        seed_crf: if >= 0, start with a narrow window around this value.
        seed_lo: optional lower bound for the seeded phase.
        seed_hi: optional upper bound for the seeded phase.

    Returns:
        A :class:`CrfResult` with the chosen CRF, estimated bitrate,
        and optionally a temp file (when *full_encode* is True).
    """
    state = _SearchState()

    # Pre-populate proven bounds from any seed_known data points so that
    # the binary search range is tighter from the very first iteration.
    if seed_known:
        if full_encode:
            accept_lo_ck = windows.final_lo
            accept_hi_ck = windows.final_hi
        else:
            accept_lo_ck = windows.sample_lo
            accept_hi_ck = windows.sample_hi
        for sk_crf, sk_bitrate in seed_known:
            log.info(
                "  Binary search seeded with known point: CRF=%d -> %d kbps",
                sk_crf, sk_bitrate,
            )
            if sk_bitrate > accept_hi_ck:
                # This CRF overshoots — proven too-high bound
                if state.proven_too_high_crf < 0 or sk_crf < state.proven_too_high_crf:
                    state.proven_too_high_crf = sk_crf
            elif sk_bitrate < accept_lo_ck:
                # This CRF undershoots — proven too-low bound
                if state.proven_too_low_crf < 0 or sk_crf > state.proven_too_low_crf:
                    state.proven_too_low_crf = sk_crf

    phases: list[tuple[int, int, str]] = []

    if seed_crf >= 0:
        s_lo = max(crf_min, seed_crf - 5)
        s_hi = min(crf_max, seed_crf + 5)
        if seed_lo >= 0:
            s_lo = max(s_lo, seed_lo)
        if seed_hi >= 0:
            s_hi = min(s_hi, seed_hi)
        phases.append((s_lo, s_hi, f"seeded CRF={seed_crf} [{s_lo}, {s_hi}]"))

    # Dynamic fallback phase — placeholder, computed after phase 1
    phases.append((-1, -1, "DYNAMIC"))

    for phase_lo, phase_hi, label in phases:
        if label == "DYNAMIC":
            # Compute expanded range from proven bounds
            lo = (
                state.proven_too_high_crf + 1
                if state.proven_too_high_crf >= 0
                else crf_min
            )
            hi = (
                state.proven_too_low_crf - 1
                if state.proven_too_low_crf >= 0
                else crf_max
            )
            lo = max(crf_min, lo)
            hi = min(crf_max, hi)

            if lo > hi:
                log.info("  Expanded range [%d, %d] is empty, skipping", lo, hi)
                continue

            label = (
                f"expanded [{lo}, {hi}] "
                f"(bounds: high@{state.proven_too_high_crf}, "
                f"low@{state.proven_too_low_crf})"
            )
        else:
            lo, hi = phase_lo, phase_hi

        found = _binary_search_phase(
            input_path, lo, hi, windows, extra_args, audio_bitrate,
            preset, audio_bitrate_kbps, max_iterations, state,
            offsets=offsets, seg_duration=seg_duration,
            full_encode=full_encode, label=label, has_audio=has_audio,
        )

        if found:
            break

        # If seeded phase didn't find a sweet-spot, expand — but keep
        # proven bounds and don't discard a best that may still be usable
        if label.startswith("seeded"):
            log.info("  No sweet-spot in seeded phase, expanding search...")
            state.clear_best()

    if state.best_crf < 0:
        log.warning("  Binary search did not converge, falling back to CRF %d", crf_max)
        state.best_crf = crf_max
        state.best_bitrate = 0
        if state.best_temp_file and state.best_temp_file.exists():
            state.best_temp_file.unlink(missing_ok=True)
            state.best_temp_file = None

    log.info(
        "  Selected CRF=%d (estimated %d kbps)", state.best_crf, state.best_bitrate
    )
    return CrfResult(
        crf=state.best_crf,
        estimated_bitrate=state.best_bitrate,
        temp_file=state.best_temp_file,
    )


# ── Interpolation ────────────────────────────────────────────────────────────

def interpolate_crf(
    crf1: int,
    bitrate1: int,
    crf2: int,
    bitrate2: int,
    target_bitrate: int,
    crf_min: int,
    crf_max: int,
) -> int:
    """Estimate the CRF for a target bitrate using log-linear interpolation.

    The relationship between CRF and log(bitrate) is approximately linear,
    so we interpolate in log-space for a better estimate.

    Returns a CRF value clamped to ``[crf_min, crf_max]``.
    """
    if bitrate1 <= 0 or bitrate2 <= 0 or bitrate1 == bitrate2:
        return (crf1 + crf2) // 2

    log_b1 = math.log(bitrate1)
    log_b2 = math.log(bitrate2)
    log_target = math.log(target_bitrate)

    if log_b1 == log_b2:
        return (crf1 + crf2) // 2

    crf_est = crf1 + (crf2 - crf1) * (log_b1 - log_target) / (log_b1 - log_b2)
    return max(crf_min, min(crf_max, round(crf_est)))


def find_optimal_crf_interpolated(
    input_path: Path,
    windows: BitrateWindows,
    extra_args: list[str],
    audio_bitrate: str,
    preset: int,
    audio_bitrate_kbps: int,
    max_iterations: int,
    crf_min: int,
    crf_max: int,
    *,
    offsets: list[float] | None = None,
    seg_duration: float = 3.0,
    full_encode: bool = False,
    seed_crf: int = -1,
    seed_lo: int = -1,
    seed_hi: int = -1,
    seed_known: list[tuple[int, int]] | None = None,
    has_audio: bool = True,
) -> CrfResult:
    """Find optimal CRF using log-linear interpolation with binary search fallback.

    Probes two initial CRF values, interpolates, and refines.  Falls back
    to binary search if interpolation doesn't converge within a few steps.

    When *seed_crf* is provided, the initial probes are placed around the
    seed value for faster convergence.

    When *seed_known* is provided, those ``(crf, bitrate)`` pairs are
    injected into the known-points table before the first probe, so that
    data already collected (e.g. from a preceding full encode) immediately
    guides the interpolation rather than being discarded.  Points whose
    bitrate falls in the acceptable range also pre-populate *best_crf* /
    *best_bitrate*, potentially allowing the search to skip encodes
    entirely if the seeded result is already good enough.
    """
    if full_encode:
        accept_lo = windows.final_lo
        accept_hi = windows.final_hi
        confident_lo = windows.final_accept_lo
        confident_hi = windows.final_accept_hi
    else:
        accept_lo = windows.sample_lo
        accept_hi = windows.sample_hi
        confident_lo = windows.sample_confident_lo
        confident_hi = windows.sample_confident_hi

    # Collect known data points
    known: list[_KnownPoint] = []
    best_crf = -1
    best_bitrate = 0
    tried_crfs: set[int] = set()

    def _cleanup_except(winner: Path | None) -> None:
        """Delete every temp file in *known* except *winner*."""
        for p in known:
            if p.temp_file and p.temp_file != winner and p.temp_file.exists():
                p.temp_file.unlink(missing_ok=True)

    # Inject any pre-existing measurements (e.g. from a prior full encode)
    # so the interpolation starts with real data instead of blind probes.
    if seed_known:
        for sk_crf, sk_bitrate in seed_known:
            log.info(
                "  Interpolation seeded with known point: CRF=%d -> %d kbps",
                sk_crf, sk_bitrate,
            )
            known.append(_KnownPoint(crf=sk_crf, bitrate=sk_bitrate))
            tried_crfs.add(sk_crf)
            if accept_lo <= sk_bitrate <= accept_hi:
                if best_crf < 0 or sk_crf < best_crf:
                    best_crf = sk_crf
                    best_bitrate = sk_bitrate

    # Initial probes at two spread-out points
    if seed_crf >= 0:
        # Probe around the seed for faster convergence.
        #
        # If seed_known is provided we can infer the required search direction:
        #   - seed bitrate > target  → seed CRF is too low  → only probe higher CRFs
        #   - seed bitrate < accept_lo → seed CRF is too high → only probe lower CRFs
        # In either case the seeded point itself already acts as one bracket, so
        # a single additional probe in the correct direction is sufficient.
        lo_bound = seed_lo if seed_lo >= 0 else crf_min
        hi_bound = seed_hi if seed_hi >= 0 else crf_max

        if seed_known:
            # Determine direction from the seeded measurement.
            sk_crf, sk_bitrate = seed_known[0]
            if sk_bitrate > accept_hi:
                # Seed overshoots — must go higher (larger CRF, lower bitrate).
                probe_crfs = [min(hi_bound, sk_crf + 3)]
            elif sk_bitrate < accept_lo:
                # Seed undershoots — must go lower (smaller CRF, higher bitrate).
                probe_crfs = [max(lo_bound, sk_crf - 3)]
            else:
                # Seed is already in range; shouldn't normally happen in precise
                # mode, but handle gracefully with a single probe toward target.
                probe_crfs = [sk_crf]
        else:
            probe_crfs = [
                max(lo_bound, seed_crf - 3),
                min(hi_bound, seed_crf + 3),
            ]
    else:
        # Empirically, optimal CRF almost always falls in [24, 50].
        # Seed the two bracketing probes at the 25th and 75th percentile
        # of that sweet-spot range rather than the full [crf_min, crf_max]
        # span, so the first interpolation step lands much closer to the
        # target without wasting an encode at a very low CRF (high bitrate).
        _SWEET_LO, _SWEET_HI = 24, 50
        sweet_lo = max(crf_min, _SWEET_LO)
        sweet_hi = min(crf_max, _SWEET_HI)
        probe_crfs = [
            sweet_lo + (sweet_hi - sweet_lo) // 4,      # ~30 — low-CRF anchor
            sweet_lo + 3 * (sweet_hi - sweet_lo) // 4,  # ~44 — high-CRF anchor
        ]

    interpolation_iters = max(max_iterations, 6)  # allow a few extra for convergence

    for iteration in range(1, interpolation_iters + 1):
        # Determine which CRF to try
        if iteration <= len(probe_crfs):
            crf = probe_crfs[iteration - 1]
        elif len(known) >= 2:
            # Find the two closest bracketing points
            above = [p for p in known if p.bitrate > accept_hi]
            below = [p for p in known if p.bitrate < accept_lo]
            in_range = [p for p in known if accept_lo <= p.bitrate <= accept_hi]

            if in_range:
                # Already have an acceptable result — try to improve by
                # interpolating between best_crf (the highest-CRF / lowest-
                # bitrate accepted result) and the nearest overshoot above it.
                # Using best_crf as the lower anchor (rather than the lowest
                # CRF seen in range) ensures we probe the gap between the
                # current best and the overshoot boundary, rather than
                # driving further below an already-acceptable lower CRF.
                if above:
                    # Use the tightest upper bracket: the highest CRF that
                    # still overshoots (closest to the acceptable range).
                    nearest_above = max(above, key=lambda x: x.crf)

                    # If best_crf and nearest_above are consecutive integers,
                    # there is no integer CRF left to probe between them —
                    # interpolation cannot improve the result further.
                    if best_crf - nearest_above.crf <= 1:
                        log.info(
                            "  Interpolation brackets are consecutive CRFs "
                            "(above=%d @ %d kbps, best=%d @ %d kbps) — "
                            "no integer CRF to probe, stopping",
                            nearest_above.crf, nearest_above.bitrate,
                            best_crf, best_bitrate,
                        )
                        break

                    crf = interpolate_crf(
                        best_crf, best_bitrate,
                        nearest_above.crf, nearest_above.bitrate,
                        windows.target, crf_min, crf_max,
                    )
                    # Clamp strictly inside the bracket — interpolation can
                    # overshoot if best_bitrate > windows.target, producing a
                    # CRF outside [nearest_above.crf, best_crf].
                    crf = max(nearest_above.crf + 1, min(best_crf - 1, crf))
                else:
                    break  # no room to improve
            elif above and below:
                # Use the tightest brackets on each side:
                #   nearest_above = highest CRF still above target (tightest upper bound)
                #   nearest_below = lowest CRF that undershoots (tightest lower bound)
                nearest_above = max(above, key=lambda x: x.crf)
                nearest_below = min(below, key=lambda x: x.crf)

                # If the two brackets are consecutive CRFs there is no integer
                # CRF left to try between them — interpolation cannot converge.
                # Return nearest_above directly as the best achievable result
                # (tightest overshoot, closest to in-range).
                if nearest_below.crf - nearest_above.crf <= 1:
                    log.info(
                        "  Interpolation brackets are consecutive CRFs "
                        "(above=%d @ %d kbps, below=%d @ %d kbps) — "
                        "no integer CRF to probe; returning nearest overshoot CRF=%d",
                        nearest_above.crf, nearest_above.bitrate,
                        nearest_below.crf, nearest_below.bitrate,
                        nearest_above.crf,
                    )
                    _cleanup_except(nearest_above.temp_file)
                    return CrfResult(
                        crf=nearest_above.crf,
                        estimated_bitrate=nearest_above.bitrate,
                        temp_file=nearest_above.temp_file,
                    )

                crf = interpolate_crf(
                    nearest_below.crf, nearest_below.bitrate,
                    nearest_above.crf, nearest_above.bitrate,
                    windows.target, crf_min, crf_max,
                )
                # Clamp strictly inside the bracket — interpolation can
                # overshoot if nearest_below.bitrate > windows.target, producing a
                # CRF outside [nearest_above.crf, nearest_below.crf].
                crf = max(nearest_above.crf + 1, min(nearest_below.crf - 1, crf))
            elif above:
                # All probes too high — extrapolate to a higher CRF.
                # Use the two extreme known points (lowest and highest CRF) to
                # fit the log-linear slope; if only one point exists, pair it
                # with crf_max as a virtual anchor (bitrate ~1 there ensures the
                # extrapolated CRF lands well past the current maximum tried).
                max_tried = max(p.crf for p in known)
                if len(known) >= 2:
                    anchor_lo = min(known, key=lambda x: x.crf)
                    anchor_hi = max(known, key=lambda x: x.crf)
                    crf = interpolate_crf(
                        anchor_lo.crf, anchor_lo.bitrate,
                        anchor_hi.crf, anchor_hi.bitrate,
                        accept_hi, crf_min, crf_max,
                    )
                else:
                    # Single point: extrapolate using crf_max as a virtual anchor.
                    # Assume bitrate at crf_max is ~1 kbps (effectively zero) so
                    # the log-linear slope points well past the current point.
                    p0 = known[0]
                    crf = interpolate_crf(
                        p0.crf, p0.bitrate,
                        crf_max, 1,
                        accept_hi, crf_min, crf_max,
                    )
                # Must be strictly above max_tried; fall back to +5 if not.
                if crf <= max_tried:
                    crf = min(crf_max, max_tried + 5)
            else:
                # All probes too low — extrapolate to a lower CRF.
                # Mirror of the above branch: use the two extreme known points,
                # or pair with crf_min (bitrate → very high) as a virtual anchor.
                min_tried = min(p.crf for p in known)
                if len(known) >= 2:
                    anchor_lo = min(known, key=lambda x: x.crf)
                    anchor_hi = max(known, key=lambda x: x.crf)
                    crf = interpolate_crf(
                        anchor_lo.crf, anchor_lo.bitrate,
                        anchor_hi.crf, anchor_hi.bitrate,
                        accept_lo, crf_min, crf_max,
                    )
                else:
                    # Single point: extrapolate using crf_min as a virtual anchor.
                    # Use a very high synthetic bitrate so the slope points well
                    # below the current point.  10× the known bitrate is a
                    # conservative but effective stand-in.
                    p0 = known[0]
                    synthetic_hi = p0.bitrate * 10
                    crf = interpolate_crf(
                        crf_min, synthetic_hi,
                        p0.crf, p0.bitrate,
                        accept_lo, crf_min, crf_max,
                    )
                # Must be strictly below min_tried; fall back to -5 if not.
                if crf >= min_tried:
                    crf = max(crf_min, min_tried - 5)
        else:
            break

        crf = max(crf_min, min(crf_max, crf))
        if crf in tried_crfs:
            # Avoid re-testing the same CRF; nudge by 1
            if crf + 1 <= crf_max and crf + 1 not in tried_crfs:
                crf += 1
            elif crf - 1 >= crf_min and crf - 1 not in tried_crfs:
                crf -= 1
            else:
                break
        tried_crfs.add(crf)

        bitrate, temp_file = _evaluate_crf_sample(
            input_path, crf, extra_args, audio_bitrate, preset,
            audio_bitrate_kbps,
            offsets=offsets, seg_duration=seg_duration,
            full_encode=full_encode, has_audio=has_audio,
        )

        if bitrate < 0:
            log.warning("  Interpolation iter %d: CRF=%d -> encode failed", iteration, crf)
            if temp_file and temp_file.exists():
                temp_file.unlink(missing_ok=True)
            continue

        log.info("  Interpolation iter %d: CRF=%d -> %d kbps", iteration, crf, bitrate)
        known.append(_KnownPoint(crf=crf, bitrate=bitrate, temp_file=temp_file))

        if accept_lo <= bitrate <= accept_hi:
            is_new_best = best_crf < 0 or crf < best_crf
            if is_new_best:
                best_crf = crf
                best_bitrate = bitrate

            if confident_lo <= bitrate <= confident_hi:
                log.info("  Interpolation converged in confident zone")
                break

    # Resolve the temp file for the winner from the known-points table,
    # then delete every other temp file that was accumulated during the search.
    best_temp_file: Path | None = None
    if best_crf >= 0:
        for p in known:
            if p.crf == best_crf:
                best_temp_file = p.temp_file
                break
    _cleanup_except(best_temp_file)

    if best_crf < 0:
        log.warning("  Interpolation did not converge, falling back to binary search")
        return find_optimal_crf(
            input_path, windows, extra_args, audio_bitrate, preset,
            audio_bitrate_kbps, max_iterations, crf_min, crf_max,
            offsets=offsets, seg_duration=seg_duration,
            full_encode=full_encode,
            seed_crf=seed_crf, seed_lo=seed_lo, seed_hi=seed_hi,
            seed_known=seed_known, has_audio=has_audio,
        )

    log.info("  Selected CRF=%d (estimated %d kbps)", best_crf, best_bitrate)
    return CrfResult(crf=best_crf, estimated_bitrate=best_bitrate, temp_file=best_temp_file)