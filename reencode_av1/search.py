"""CRF search: outer loop with pluggable stateless search methods.

The outer ``find_optimal_crf`` function manages all state (history, temp
files, accept/confident checks, max-crf ceiling fallback, crf-min floor,
the running overhead estimate) and delegates the choice of *which* CRF to
probe next to a stateless search method.  Each search method receives the
full history of measured ``CrfPoint`` results plus a :class:`SearchContext`
(bounds, target windows, seed, and the overhead estimate), and returns
either an integer CRF to probe next, or ``None`` to stop searching.

The interface between the outer loop and the search methods is deliberately
narrow: the outer loop works entirely in *total* (whole-file) bitrate — every
recorded ``CrfPoint.bitrate`` and every window check is total — while a search
method works in *video* space internally, subtracting ``SearchContext.overhead``
before its log-linear interpolation (only the video component is log-linear in
CRF). Classification of points as over/under target stays in total space.

Three search methods are provided: :func:`smart_search_next` (the
default), :func:`binary_search_next` and :func:`interpolation_next`.
Adding a new strategy is a matter of writing another function with the
same signature.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .encode import encode_sample, encode_segments
from .filters import BitrateWindows

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CrfPoint:
    """A measured point in the CRF search: a CRF and the bitrate it produced.

    CRF and bitrate move in opposite directions (higher CRF → lower
    bitrate), so pairing them in a named type avoids the ambiguity of a
    bare ``(int, int)`` tuple where it's easy to lose track of which is
    which.
    """

    crf: int
    bitrate: int


@dataclass
class CrfResult:
    """Result of a CRF search."""

    crf: int
    estimated_bitrate: int
    temp_file: Path | None = None
    crf_ceiling_used: bool = False


@dataclass(frozen=True)
class SearchContext:
    """The inputs a stateless search method needs to pick the next CRF.

    ``crf_min``/``crf_max`` bound the search; ``accept_lo``/``accept_hi`` and
    ``confident_lo``/``confident_hi`` are the (total-bitrate) acceptance and
    convergence windows; ``seed_crf`` is an optional first-probe hint (-1 when
    unset); ``overhead`` is the estimated audio+container bitrate (0 when not
    yet measured), subtracted before any log-linear interpolation so it operates
    on the video component alone. The confident bounds are used only by
    :func:`smart_search_next`; ``overhead`` only by the interpolating methods.
    """

    crf_min: int
    crf_max: int
    accept_lo: int
    accept_hi: int
    confident_lo: int = 0
    confident_hi: int = 0
    seed_crf: int = -1
    overhead: int = 0


# A stateless search method.  Returns the next CRF to probe, or None to stop.
SearchMethod = Callable[[list[CrfPoint], SearchContext], "int | None"]


# ── Sampling helper ─────────────────────────────────────────────────────────

def _evaluate_crf_sample(
    input_path: Path,
    crf: int,
    extra_args: list[str],
    audio_bitrate: str,
    preset: int,
    *,
    offsets: list[float] | None,
    seg_duration: float,
    full_encode: bool,
    has_audio: bool = True,
    measure_overhead: bool = False,
) -> tuple[int, int | None, Path | None]:
    """Evaluate a CRF value using the appropriate sampling method.

    Returns ``(total_bitrate_kbps, overhead_kbps_or_none, temp_file_or_none)``.
    """
    if full_encode:
        return encode_sample(
            input_path, crf, extra_args, audio_bitrate, preset,
            keep_file=True, has_audio=has_audio, measure_overhead=measure_overhead,
        )
    if offsets:
        bitrate, overhead = encode_segments(
            input_path, crf, extra_args, audio_bitrate, preset,
            offsets, seg_duration, has_audio=has_audio,
            measure_overhead=measure_overhead,
        )
        return bitrate, overhead, None
    # Single-segment fallback (shouldn't normally happen)
    bitrate, overhead, _ = encode_sample(
        input_path, crf, extra_args, audio_bitrate, preset,
        duration=seg_duration, has_audio=has_audio,
        measure_overhead=measure_overhead,
    )
    return bitrate, overhead, None


def _select_windows(
    windows: BitrateWindows,
    full_encode: bool,
) -> tuple[int, int, int, int]:
    """Return ``(accept_lo, accept_hi, confident_lo, confident_hi)``."""
    if full_encode:
        return (
            windows.final_lo, windows.final_hi,
            windows.final_accept_lo, windows.final_accept_hi,
        )
    return (
        windows.sample_lo, windows.sample_hi,
        windows.sample_confident_lo, windows.sample_confident_hi,
    )


# ── Log-linear interpolation primitive ──────────────────────────────────────

def interpolate_crf(
    p1: CrfPoint,
    p2: CrfPoint,
    target_bitrate: int,
    crf_min: int,
    crf_max: int,
    overhead: int = 0,
) -> int:
    """Estimate the CRF for a target bitrate using log-linear interpolation.

    The relationship between CRF and ``log(bitrate)`` is approximately
    linear for the *video* component only, so ``overhead`` (the constant
    audio+container bitrate) is subtracted from both measured points and the
    target before interpolating in log-space; this removes the curvature an
    additive constant would otherwise introduce.  With ``overhead == 0`` this
    is plain log-linear interpolation.  The result is clamped to
    ``[crf_min, crf_max]``; if subtracting the overhead leaves a non-positive
    video bitrate anywhere, it falls back to the CRF midpoint.
    """
    v1 = p1.bitrate - overhead
    v2 = p2.bitrate - overhead
    vt = target_bitrate - overhead
    if v1 <= 0 or v2 <= 0 or vt <= 0 or v1 == v2:
        return (p1.crf + p2.crf) // 2

    log_b1 = math.log(v1)
    log_b2 = math.log(v2)
    log_target = math.log(vt)

    if log_b1 == log_b2:
        return (p1.crf + p2.crf) // 2

    crf_est = p1.crf + (p2.crf - p1.crf) * (log_b1 - log_target) / (log_b1 - log_b2)
    return max(crf_min, min(crf_max, round(crf_est)))


def _extrapolate_crf(
    history: list[CrfPoint],
    target_bitrate: int,
    crf_min: int,
    crf_max: int,
    *,
    direction: int,
    crf_nudge_size: int = 5,
    overhead: int = 0,
) -> int:
    """Extrapolate a CRF when all known points are on one side of the target.

    *direction* must be +1 (all overshooting → need higher CRF) or -1 (all
    undershooting → need lower CRF). Virtual anchors are expressed in total
    bitrate (overhead added back) so that the video component drives them.
    """
    anchor_lo = min(history, key=lambda p: p.crf)
    anchor_hi = max(history, key=lambda p: p.crf)

    if anchor_lo.crf == anchor_hi.crf:
        point = history[0]
        if direction == 1:
            # Virtual anchor at crf_max with near-zero video bitrate to push higher
            crf = interpolate_crf(point, CrfPoint(crf_max, overhead + 1), target_bitrate, crf_min, crf_max, overhead)
        else:
            # Virtual anchor at crf_min with 10x the video bitrate to push lower
            virtual_lo = CrfPoint(crf_min, overhead + (point.bitrate - overhead) * 10)
            crf = interpolate_crf(virtual_lo, point, target_bitrate, crf_min, crf_max, overhead)
    else:
        crf = interpolate_crf(anchor_lo, anchor_hi, target_bitrate, crf_min, crf_max, overhead)

    # If the estimate didn't move past the extreme tried point, nudge it.
    if direction == 1:
        if crf <= anchor_hi.crf:
            crf = min(crf_max, anchor_hi.crf + crf_nudge_size)
    else:
        if crf >= anchor_lo.crf:
            crf = max(crf_min, anchor_lo.crf - crf_nudge_size)

    return crf


# ── Stateless search methods ────────────────────────────────────────────────

def binary_search_next(
    history: list[CrfPoint],
    ctx: SearchContext,
) -> int | None:
    """Choose the next CRF via binary search over proven bounds.

    The confident-window bounds and ``overhead`` on *ctx* are part of the
    shared :data:`SearchMethod` interface but are not used by this method.

    Derives the current ``[lo, hi]`` range from the history:
      * The lowest CRF whose bitrate overshoots ``accept_hi`` becomes a
        strict lower bound on the next probe (lo = that CRF + 1).
      * The highest CRF whose bitrate undershoots ``accept_lo`` (or the
        lowest in-range CRF, when one exists) becomes a strict upper
        bound (hi = that CRF − 1).

    Returns the midpoint of the resulting range, or ``None`` if the range
    is empty.  When the history is empty, ``ctx.seed_crf`` (if in range) is
    used as the first probe; otherwise the midpoint of ``[crf_min, crf_max]``.
    """
    crf_min, crf_max = ctx.crf_min, ctx.crf_max
    accept_lo, accept_hi = ctx.accept_lo, ctx.accept_hi
    seed_crf = ctx.seed_crf

    overshoot_crfs = [p.crf for p in history if p.bitrate > accept_hi]
    undershoot_crfs = [p.crf for p in history if p.bitrate < accept_lo]
    in_range_crfs = [p.crf for p in history if accept_lo <= p.bitrate <= accept_hi]

    lo = (max(overshoot_crfs) + 1) if overshoot_crfs else crf_min
    if in_range_crfs:
        # Try lower CRFs (higher quality) than the best in-range point.
        hi = min(in_range_crfs) - 1
    elif undershoot_crfs:
        hi = min(undershoot_crfs) - 1
    else:
        hi = crf_max

    lo = max(crf_min, lo)
    hi = min(crf_max, hi)

    if lo > hi:
        return None

    if not history and seed_crf >= 0 and lo <= seed_crf <= hi:
        return seed_crf

    return (lo + hi) // 2


def interpolation_next(
    history: list[CrfPoint],
    ctx: SearchContext,
) -> int | None:
    """Choose the next CRF via log-linear interpolation.

    With < 2 history points, returns probe CRFs designed to bracket the
    target quickly.  With 2+ points, interpolates between bracketing
    points or extrapolates when all points are on one side.  Returns
    ``None`` when no useful integer CRF remains to probe (e.g. the
    bracketing points are consecutive integers).

    The confident-window bounds on *ctx* are part of the shared
    :data:`SearchMethod` interface but are not used by this method;
    ``ctx.overhead`` is passed to the interpolation so it works on the video
    component.
    """
    crf_min, crf_max = ctx.crf_min, ctx.crf_max
    accept_lo, accept_hi = ctx.accept_lo, ctx.accept_hi
    seed_crf, overhead = ctx.seed_crf, ctx.overhead
    target_bitrate = accept_hi

    if len(history) == 0:
        if seed_crf >= 0:
            return max(crf_min, min(crf_max, seed_crf - 3))
        # Probe near the 25th percentile of the empirical sweet-spot [24, 50].
        return max(crf_min, min(crf_max, 30))

    if len(history) == 1:
        point = history[0]
        if point.bitrate > accept_hi:
            return min(crf_max, point.crf + 3)
        if point.bitrate < accept_lo:
            return max(crf_min, point.crf - 3)
        # In range: probe a lower CRF to try for higher quality, still in window.
        return max(crf_min, point.crf - 3)

    # 2+ points: bracket / interpolate.
    above = [p for p in history if p.bitrate > accept_hi]
    below = [p for p in history if p.bitrate < accept_lo]
    in_range = [p for p in history if accept_lo <= p.bitrate <= accept_hi]

    if in_range:
        if not above:
            # No overshoot to bracket against — extrapolate toward lower CRF.
            lowest_in_range_crf = min(p.crf for p in in_range)
            if lowest_in_range_crf <= crf_min:
                return None
            crf = _extrapolate_crf(
                history, target_bitrate, crf_min, crf_max,
                direction=-1, crf_nudge_size=2, overhead=overhead,
            )
            return max(crf_min, min(lowest_in_range_crf - 1, crf))

        nearest_above = min(above, key=lambda p: p.bitrate)  # smallest overshoot
        lowest_in_range = min(in_range, key=lambda p: p.crf)
        if lowest_in_range.crf - nearest_above.crf <= 1:
            return None  # consecutive integers — no untried CRF in between
        crf = interpolate_crf(
            lowest_in_range, nearest_above,
            target_bitrate, crf_min, crf_max, overhead,
        )
        return max(nearest_above.crf + 1, min(lowest_in_range.crf - 1, crf))

    if above and below:
        nearest_above = min(above, key=lambda p: p.bitrate)  # smallest overshoot
        nearest_below = max(below, key=lambda p: p.bitrate)  # smallest undershoot
        if nearest_below.crf - nearest_above.crf <= 1:
            return None
        crf = interpolate_crf(
            nearest_below, nearest_above,
            target_bitrate, crf_min, crf_max, overhead,
        )
        return max(nearest_above.crf + 1, min(nearest_below.crf - 1, crf))

    if above:
        return _extrapolate_crf(history, target_bitrate, crf_min, crf_max, direction=1, overhead=overhead)

    if below:
        return _extrapolate_crf(history, target_bitrate, crf_min, crf_max, direction=-1, overhead=overhead)

    return None


def smart_search_next(
    history: list[CrfPoint],
    ctx: SearchContext,
) -> int | None:
    """Choose the next CRF via bracket-then-interpolate (the default method).

    Assumes monotonicity (bitrate decreases as CRF increases) and aims at
    the centre of the confident window — ``(confident_lo + confident_hi)
    // 2`` — which is the same convergence zone the outer loop checks
    against (``sample_confident_*`` for segment sampling,
    ``final_accept_*`` for full-video encodes).

    The strategy:

      * First probe (empty history): the midpoint of ``[crf_min, crf_max]``.
      * One-sided history (every probe overshoots, or every probe
        undershoots, the target): jump straight to the opposite bound —
        ``crf_max`` when overshooting (CRF too small), ``crf_min`` when
        undershooting (CRF too large) — to establish a bracket the way a
        binary search would.  Returns ``None`` once the relevant bound has
        already been probed (no untried CRF can improve on the bracket).
      * Bracketed history (points on both sides of the target):
        log-linear interpolation (in video space, via ``ctx.overhead``)
        between the closest pair straddling the target, clamped to lie
        strictly between them.  Returns ``None`` when that pair is already
        consecutive (no integer CRF in between).
    """
    crf_min, crf_max = ctx.crf_min, ctx.crf_max
    target = (ctx.confident_lo + ctx.confident_hi) // 2

    if not history:
        return (crf_min + crf_max) // 2

    over = [p for p in history if p.bitrate > target]   # bitrate too high → CRF too small
    under = [p for p in history if p.bitrate <= target]  # bitrate too low → CRF too large

    if over and under:
        # The pair tightest around the target: smallest overshoot (highest
        # CRF on the over side) and smallest undershoot (lowest CRF on the
        # under side).  By monotonicity lo_pt.crf < hi_pt.crf.
        lo_pt = min(over, key=lambda p: p.bitrate)
        hi_pt = max(under, key=lambda p: p.bitrate)
        if hi_pt.crf - lo_pt.crf <= 1:
            return None  # consecutive integers — nothing left to probe between them
        crf = interpolate_crf(lo_pt, hi_pt, target, crf_min, crf_max, ctx.overhead)
        return max(lo_pt.crf + 1, min(hi_pt.crf - 1, crf))

    if over:
        # Everything overshoots → need a higher CRF; jump to the ceiling.
        if max(p.crf for p in over) >= crf_max:
            return None
        return crf_max

    # Everything undershoots → need a lower CRF; jump to the floor.
    if min(p.crf for p in under) <= crf_min:
        return None
    return crf_min


# ── Outer loop ──────────────────────────────────────────────────────────────

def find_optimal_crf(
    input_path: Path,
    windows: BitrateWindows,
    extra_args: list[str],
    audio_bitrate: str,
    preset: int,
    max_iterations: int,
    crf_min: int,
    crf_max: int,
    crf_ceiling_fallback: int,
    *,
    search_method: SearchMethod = smart_search_next,
    offsets: list[float] | None = None,
    seg_duration: float = 5.0,
    full_encode: bool = False,
    seed_crf: int = -1,
    seed_known: list[CrfPoint] | None = None,
    seed_temp_files: dict[int, Path] | None = None,
    seed_overhead: int | None = None,
    has_audio: bool = True,
) -> CrfResult:
    """Find the optimal CRF using the configured stateless search method.

    The outer loop:

    1. Asks *search_method* for the next CRF to probe.  ``None`` stops
       the search.
    2. If the candidate CRF was already tried, nudges it by ±1 in the
       direction that brings the bitrate closer to the target.  If the
       nudged CRF is also tried (or out of bounds), the search stops
       and returns the most appropriate of the two.
    3. Encodes at the candidate CRF and records the result in *history*.
    4. If the bitrate is in the confident window, returns immediately.
    5. If the CRF is at/above ``crf_max`` and the bitrate still exceeds
       the target, returns ``crf_ceiling_fallback`` with
       ``crf_ceiling_used=True``.
    6. If the CRF is at/below ``crf_min`` and the bitrate is still below
       ``accept_lo``, returns the ``crf_min`` result.

    If ``max_iterations`` is exhausted without convergence, returns the
    best point from history (preferring in-range, then closest from
    below the target, then closest from above).
    """
    accept_lo, accept_hi, confident_lo, confident_hi = _select_windows(windows, full_encode)
    target_bitrate = accept_hi

    # Audio+container overhead, measured once per phase from the first completed
    # encode and reused for the rest. A caller-supplied ``seed_overhead`` (the
    # precise pass passes the value measured from the pre-search full encode)
    # means it's already established and no measurement is requested.
    overhead: int | None = seed_overhead

    history: list[CrfPoint] = list(seed_known or [])
    tried_crfs: set[int] = {p.crf for p in history}
    # Seeded points may carry a reusable encode (e.g. a full-video encode the
    # caller already produced at that CRF), so the search needn't redo it if it
    # ends up selecting that point.  These participate in the same lifecycle as
    # encodes made during the loop, so a non-winning seed file is cleaned up too.
    temp_files_by_crf: dict[int, Path] = dict(seed_temp_files or {})

    log.info(
        "  CRF search: target=%d, accept=[%d, %d], confident=[%d, %d], CRF=[%d, %d]",
        windows.target, accept_lo, accept_hi, confident_lo, confident_hi,
        crf_min, crf_max,
    )
    if seed_known:
        for point in seed_known:
            log.info("  Seeded with known point: CRF=%d -> %d kbps", point.crf, point.bitrate)

    def _cleanup_except(keep_crf: int | None) -> None:
        for c, p in list(temp_files_by_crf.items()):
            if c != keep_crf and p.exists():
                p.unlink(missing_ok=True)
            if c != keep_crf:
                temp_files_by_crf.pop(c, None)

    def _best_from_history() -> CrfPoint | None:
        if not history:
            return None
        in_range = [p for p in history if accept_lo <= p.bitrate <= accept_hi]
        if in_range:
            return min(in_range, key=lambda p: p.crf)  # lowest CRF (highest quality)
        below = [p for p in history if p.bitrate <= accept_hi]
        if below:
            return max(below, key=lambda p: p.bitrate)  # closest to target from below
        return max(history, key=lambda p: p.crf)  # closest to target from above

    def _return_best() -> CrfResult:
        best = _best_from_history()
        if best is None:
            log.warning(
                "  Search did not converge and no history; falling back to CRF=%d",
                crf_max,
            )
            _cleanup_except(None)
            return CrfResult(crf_max, 0, None, crf_ceiling_used=False)
        log.info("  Selected CRF=%d (estimated %d kbps)", best.crf, best.bitrate)
        _cleanup_except(best.crf)
        return CrfResult(best.crf, best.bitrate, temp_files_by_crf.get(best.crf), crf_ceiling_used=False)

    for iteration in range(1, max_iterations + 1):
        ctx = SearchContext(
            crf_min=crf_min, crf_max=crf_max,
            accept_lo=accept_lo, accept_hi=accept_hi,
            confident_lo=confident_lo, confident_hi=confident_hi,
            seed_crf=seed_crf, overhead=overhead or 0,
        )
        candidate = search_method(history, ctx)
        if candidate is None:
            log.info("  Search method has no further probes; stopping")
            break

        candidate = max(crf_min, min(crf_max, candidate))

        # Duplicate check: nudge ±1 toward the target.
        if candidate in tried_crfs:
            dup_bitrate = next(
                (p.bitrate for p in history if p.crf == candidate),
                None,
            )
            if dup_bitrate is None:
                # Previous attempt at this CRF failed (no recorded bitrate);
                # default to stepping up to a higher CRF.
                step = +1
            elif dup_bitrate > target_bitrate:
                step = +1  # bitrate too high → higher CRF (lower bitrate)
            else:
                step = -1  # bitrate ≤ target → lower CRF (higher bitrate)
            stepped = candidate + step

            stepped_in_bounds = crf_min <= stepped <= crf_max
            stepped_tried = stepped in tried_crfs

            if not stepped_in_bounds or stepped_tried:
                log.warning(
                    "  Search returned tried CRF=%d and the nudge target "
                    "(CRF=%d) is %s; stopping",
                    candidate, stepped,
                    "out of bounds" if not stepped_in_bounds else "also tried",
                )
                return _return_best()

            log.info(
                "  CRF=%d already tried; nudged to CRF=%d", candidate, stepped,
            )
            candidate = stepped

        tried_crfs.add(candidate)
        # Measure the audio+container overhead only until it's established for
        # this phase; subsequent encodes reuse the cached value.
        bitrate, measured_overhead, temp_file = _evaluate_crf_sample(
            input_path, candidate, extra_args, audio_bitrate, preset,
            offsets=offsets, seg_duration=seg_duration,
            full_encode=full_encode, has_audio=has_audio,
            measure_overhead=(overhead is None),
        )

        if bitrate < 0:
            log.warning(
                "  Iteration %d: CRF=%d -> encode failed", iteration, candidate,
            )
            if temp_file and temp_file.exists():
                temp_file.unlink(missing_ok=True)
            continue

        if measured_overhead is not None:
            overhead = measured_overhead
            log.info("  Measured overhead (audio+container): %d kbps", overhead)

        log.info(
            "  Iteration %d: CRF=%d -> %d kbps", iteration, candidate, bitrate,
        )
        history.append(CrfPoint(candidate, bitrate))
        if temp_file:
            temp_files_by_crf[candidate] = temp_file

        # Confident-zone exit.
        if confident_lo <= bitrate <= confident_hi:
            log.info(
                "  Converged in confident zone [%d, %d]",
                confident_lo, confident_hi,
            )
            _cleanup_except(candidate)
            return CrfResult(
                candidate, bitrate,
                temp_files_by_crf.get(candidate),
                crf_ceiling_used=False,
            )

        # max-crf ceiling: bitrate still too high at the quality floor.
        if candidate >= crf_max and bitrate > accept_hi:
            log.info(
                "  CRF=%d >= max-crf=%d and bitrate (%d kbps) still exceeds "
                "target (%d kbps); using crf-ceiling-fallback=%d",
                candidate, crf_max, bitrate, accept_hi, crf_ceiling_fallback,
            )
            _cleanup_except(None)
            return CrfResult(
                crf_ceiling_fallback, 0, None, crf_ceiling_used=True,
            )

        # crf-min floor: bitrate still too low at the quality ceiling.
        if candidate <= crf_min and bitrate < accept_lo:
            log.info(
                "  CRF=%d <= crf-min=%d and bitrate (%d kbps) still below "
                "accept_lo (%d kbps); stopping at crf-min",
                candidate, crf_min, bitrate, accept_lo,
            )
            _cleanup_except(candidate)
            return CrfResult(
                candidate, bitrate,
                temp_files_by_crf.get(candidate),
                crf_ceiling_used=False,
            )

    return _return_best()
