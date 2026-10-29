"""System identification helpers for the response pass.

- ``uniform_resample`` / ``resample_on``: put a logged series on a uniform time grid,
  dropping repeated and backward timestamps first.
- ``first_order_lowpass``: the causal first-order lag, exact and vectorized.
- ``fit_first_order_plant``: fit a response to its input passed through a pure
  delay and a first-order lag at every point of a delay and time constant grid, by
  least squares, or by two-stage least squares when instruments are given (for
  closed-loop data), keeping the best point.
- ``butterworth2_group_delay_s`` / ``first_order_group_delay_s``: the low-frequency
  group delay of PX4's second-order low-pass (``LowPassFilter2p``) and of its
  first-order ``AlphaFilter``; ``response.py`` says which of them sit on the logged
  angular acceleration path and cites the PX4 source.

numpy only. No function changes its inputs.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np

# first_order_lowpass works in blocks inside which the weight r**-k grows to at most
# e**_MAX_BLOCK_GROWTH: far below the float64 limit (about e**709), and the result
# keeps full precision because each block's sum is scaled back by r**k at once.
_MAX_BLOCK_GROWTH = 300.0
# The fewest response samples, over all runs, that a plant fit accepts.
MIN_FIT_SAMPLES = 100
# Tolerance when a delay is converted to a whole number of samples plus a fraction.
_SAMPLE_EPS = 1e-9
# Instrument directions whose variance is below this share of the instruments'
# uncentred energy are dropped: lags of one smooth signal are nearly collinear, and
# a constant instrument centres to rounding noise.
_COLLINEAR_FLOOR = 1e-10
# A grid point where the instruments explain less than this share of the filtered
# input (first-stage R^2) is invalid: the gain there is a ratio of rounding errors.
_NO_SIGNAL_FLOOR = 1e-9


@dataclass(frozen=True)
class PlantFit:
    """y = gain * lowpass(u, tau_s) delayed by delay_s, plus an offset; r2 in [0, 1].

    r2 is the share of y the fitted model explains. Least squares: the ordinary
    R^2, and first_stage_r2 and reduced_form_r2 are 1.0. Two-stage least squares:
    (x'Py)^2 / (x'Px y'y); first_stage_r2 is the R^2 of the filtered, delayed input
    on the instruments and reduced_form_r2 that of y, y'Py / y'y.
    """

    gain: float
    delay_s: float
    tau_s: float
    r2: float
    first_stage_r2: float = 1.0
    reduced_form_r2: float = 1.0

    @property
    def lag_s(self) -> float:
        """Effective lag: the pure delay plus the first-order time constant."""
        return self.delay_s + self.tau_s


# ---------------------------------------------------------------------------
# Resampling
# ---------------------------------------------------------------------------


def increasing_samples(t_s: np.ndarray, x: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """New arrays without the samples whose stamp is not finite or not later than all before it.

    ``x`` may be one value per stamp or one row per stamp.
    """
    t = np.asarray(t_s, dtype=float)
    values = np.asarray(x, dtype=float)
    finite = np.isfinite(t)
    t, values = t[finite], values[finite]
    if t.size == 0:
        return t, values
    keep = np.ones(t.size, dtype=bool)
    keep[1:] = t[1:] > np.maximum.accumulate(t)[:-1]
    return t[keep], values[keep]


def resample_on(t_s: np.ndarray, x: np.ndarray, grid_s: np.ndarray,
                max_gap_s: float | None = None) -> np.ndarray:
    """x linearly interpolated at grid_s from its increasing samples; NaN outside their span.

    With max_gap_s, a grid point whose two neighbouring samples are further apart than
    max_gap_s is NaN too: a logging gap is left empty, not bridged by a straight line.
    """
    t, values = increasing_samples(t_s, x)
    grid = np.asarray(grid_s, dtype=float)
    if t.size == 0:
        return np.full(grid.size, np.nan)
    out = np.interp(grid, t, values, left=np.nan, right=np.nan)
    if max_gap_s is None or t.size < 2:
        return out
    right = np.clip(np.searchsorted(t, grid, side="right"), 1, t.size - 1)
    on_sample = t[right - 1] == grid
    return np.where((t[right] - t[right - 1] > max_gap_s) & ~on_sample, np.nan, out)


def uniform_resample(
    t_s: np.ndarray, x: np.ndarray, dt_s: float
) -> tuple[np.ndarray, np.ndarray]:
    """(grid, x on grid): drop non-increasing stamps, then interpolate linearly every dt_s.

    The grid starts at the first kept stamp and ends at or before the last one.
    """
    if not dt_s > 0:
        raise ValueError(f"dt_s must be positive, got {dt_s}")
    t, values = increasing_samples(t_s, x)
    if t.size == 0:
        return np.empty(0), np.empty(0)
    n = int(math.floor((t[-1] - t[0]) / dt_s + _SAMPLE_EPS)) + 1
    grid = t[0] + np.arange(n) * dt_s
    return grid, np.interp(grid, t, values)


# ---------------------------------------------------------------------------
# First-order low-pass
# ---------------------------------------------------------------------------


def _block_entry_states(block_ends: np.ndarray, decay: float, initial: float) -> np.ndarray:
    """State entering each block: s[0] = initial, s[b + 1] = decay * s[b] + block_ends[b]."""
    states = np.empty(block_ends.size)
    state = initial
    for b, end in enumerate(block_ends):
        states[b] = state
        state = decay * state + end
    return states


def first_order_lowpass(u: np.ndarray, dt_s: float, tau_s: float) -> np.ndarray:
    """Causal first-order low-pass: y[n] = y[n-1] + a (u[n] - y[n-1]), a = dt / (tau + dt).

    The filter starts settled at u[0]; tau_s <= 0 returns a copy of u. Its
    low-frequency delay is tau_s exactly. Computed without a per-sample loop:
    inside a block, y[k] = a r**k cumsum(r**-j u[j]) with r = 1 - a, and the
    state carried into each block decays as r**(k + 1).
    """
    out = np.array(u, dtype=float)
    if tau_s <= 0 or out.size == 0:
        return out
    a = dt_s / (tau_s + dt_s)
    log_r = math.log1p(-a)
    block = max(1, min(out.size, int(_MAX_BLOCK_GROWTH / -log_r)))
    n_blocks = -(-out.size // block)
    padded = np.zeros(n_blocks * block)
    padded[: out.size] = out
    blocks = padded.reshape(n_blocks, block)
    k = np.arange(block)
    decay = np.exp(log_r * k)
    local = a * decay * np.cumsum(blocks * np.exp(-log_r * k), axis=1)
    entry = _block_entry_states(local[:, -1], math.exp(log_r * block), float(out[0]))
    filtered = local + entry[:, None] * (decay * math.exp(log_r))
    return filtered.reshape(-1)[: out.size]


# ---------------------------------------------------------------------------
# Plant fit
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _DelayGrid:
    """Each grid delay as whole + frac samples, and the samples every fit skips first."""

    whole: np.ndarray
    frac: np.ndarray
    warmup: int


def fit_warmup_samples(dt_s: float, delays_s: Sequence[float]) -> int:
    """The first samples of every run that the fit does not score: the longest delay, plus one."""
    return int(np.floor(max(delays_s) / dt_s + _SAMPLE_EPS)) + 1


@dataclass(frozen=True)
class _Prepared:
    """Centred runs and the sums that do not depend on u.

    Each run is (u, rows): rows[0] the response after warmup, rows[1:] one instrument
    each, as contiguous rows (np.dot on a contiguous row is several times faster than a
    matrix product with an (n, k) matrix, and avoids the spurious floating-point
    warnings numpy 2 on Apple Accelerate raises for some matmul shapes).
    """

    runs: list[tuple[np.ndarray, np.ndarray]]
    count: int
    syy: float
    whiten: np.ndarray | None  # W with W' (Z'Z) W = I on the kept directions
    zy_white: np.ndarray | None  # W' Z'y

    @property
    def reduced_form_r2(self) -> float:
        """y'Py / y'y: the share of y the instruments explain (1.0 without instruments)."""
        if self.zy_white is None:
            return 1.0
        return float(np.dot(self.zy_white, self.zy_white)) / self.syy


def _delay_grid(dt_s: float, delays: np.ndarray) -> _DelayGrid:
    shifts = delays / dt_s
    whole = np.floor(shifts + _SAMPLE_EPS).astype(int)
    return _DelayGrid(whole, np.clip(shifts - whole, 0.0, 1.0),
                      fit_warmup_samples(dt_s, delays))


def _delayed(per_shift: np.ndarray, grid: _DelayGrid) -> np.ndarray:
    """Per-shift sums (rows n = 0..warmup) at every grid delay, by linear interpolation:
    x = (1 - frac) A_n + frac A_(n+1), A_n the low-passed input shifted by n samples."""
    w1 = grid.frac.reshape((-1,) + (1,) * (per_shift.ndim - 1))
    return (1.0 - w1) * per_shift[grid.whole] + w1 * per_shift[grid.whole + 1]


def _shifted_sums(lp: np.ndarray, rows: np.ndarray, warmup: int
                  ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Per shift n: (sum A_n, A_n . row for each row), A_n . A_n and A_n . A_(n+1)."""
    n = np.arange(warmup + 1)
    lo, hi = warmup - n, lp.size - n
    zero = np.zeros(1)
    sums = np.concatenate((zero, np.cumsum(lp)))
    squares = np.concatenate((zero, np.cumsum(lp * lp)))
    neighbours = np.concatenate((zero, zero, np.cumsum(lp[1:] * lp[:-1])))
    cross = np.array([[np.dot(lp[a:b], row) for row in rows] for a, b in zip(lo, hi)])
    linear = np.column_stack((sums[hi] - sums[lo], cross))
    return linear, squares[hi] - squares[lo], neighbours[hi] - neighbours[lo]


def _run_sums(u: np.ndarray, rows: np.ndarray, dt_s: float, tau_s: float,
              grid: _DelayGrid) -> np.ndarray:
    """Per delay: (sum x, sum x*x, sum x*y, Z'x...) of x = u low-passed by tau_s and delayed."""
    linear, squares, neighbours = _shifted_sums(first_order_lowpass(u, dt_s, tau_s), rows,
                                                grid.warmup)
    w0, w1, n = 1.0 - grid.frac, grid.frac, grid.whole
    sxx = w0 * w0 * squares[n] + 2.0 * w0 * w1 * neighbours[n] + w1 * w1 * squares[n + 1]
    moved = _delayed(linear, grid)
    return np.column_stack((moved[:, 0], sxx, moved[:, 1:]))


def _scores(sums: np.ndarray, prepared: _Prepared
            ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(R^2, gain, first-stage R^2) per delay, all centred (the model has an offset).

    Without instruments: least squares, R^2 the squared correlation. With them:
    two-stage least squares, gain = x'Py / x'Px for P the projection on the
    instruments, and R^2 = (x'Py)^2 / (x'Px y'y): the share of y the fitted model
    explains. That is the R^2 projected on the instruments, 1 - e'Pe / y'Py, times
    y'Py / y'y, which is the same at every grid point, so both pick the same point.
    A point where the instruments explain almost none of x is invalid.
    """
    sx, sxx, sxy = sums[:, 0], sums[:, 1], sums[:, 2]
    var_x = sxx - sx * sx / prepared.count
    with np.errstate(divide="ignore", invalid="ignore"):
        if prepared.whiten is None:
            num, den, first = sxy, var_x, np.ones_like(var_x)
        else:
            q = sums[:, 3:] @ prepared.whiten
            num, den = q @ prepared.zy_white, np.sum(q * q, axis=1)
            first = np.clip(den / var_x, 0.0, 1.0)
        gain = num / den
        r2 = num * num / (den * prepared.syy)
    valid = (var_x > 0) & (first > _NO_SIGNAL_FLOOR) & np.isfinite(r2) & np.isfinite(gain)
    return np.where(valid, np.clip(r2, 0.0, 1.0), -np.inf), gain, first


def _usable_runs(runs: Sequence[tuple[np.ndarray, ...]], warmup: int
                 ) -> list[tuple[np.ndarray, np.ndarray, np.ndarray | None]]:
    """Validated (u, y, instruments or None) runs longer than warmup."""
    usable = []
    for run in runs:
        u, y = (np.asarray(a, dtype=float) for a in run[:2])
        z = np.asarray(run[2], dtype=float) if len(run) > 2 and run[2] is not None else None
        if u.ndim != 1 or y.shape != u.shape or (z is not None and (
                z.ndim != 2 or z.shape[0] != u.size)):
            raise ValueError("each run needs 1-D input and response, and (n, k) instruments, "
                             "of one length")
        if not all(np.all(np.isfinite(a)) for a in (u, y, z) if a is not None):
            raise ValueError("runs must hold finite samples only")
        if u.size > warmup:
            usable.append((u, y, z))
    if len({z is None for _, _, z in usable}) > 1:
        raise ValueError("either every run has instruments or none has")
    if sum(u.size - warmup for u, _, _ in usable) < MIN_FIT_SAMPLES:
        raise ValueError(f"too few samples to fit (need {MIN_FIT_SAMPLES} after the longest delay)")
    return usable


def _whitening(zz: np.ndarray, energy: float) -> np.ndarray:
    """W with W' zz W = I on the directions of the centred Gram zz that matter.

    A direction is kept when its variance is above _COLLINEAR_FLOOR times the
    instruments' uncentred energy, so a constant instrument (centred to rounding
    noise) keeps none. At least 2 directions must remain: the fit is over-identified.
    """
    values, vectors = np.linalg.eigh(zz)
    keep = values > _COLLINEAR_FLOOR * energy
    if np.count_nonzero(keep) < 2:
        raise ValueError("the instruments have fewer than 2 independent directions over "
                         "the fitted samples")
    return vectors[:, keep] / np.sqrt(values[keep])


def _gram(rows: np.ndarray) -> np.ndarray:
    """rows rows': every pairwise np.dot of the rows."""
    return np.array([[float(np.dot(a, b)) for b in rows] for a in rows])


def _prepare(usable: list[tuple[np.ndarray, np.ndarray, np.ndarray | None]], warmup: int
             ) -> _Prepared:
    """Centre input, response (after warmup) and instruments by their means over all runs."""
    u_all = np.concatenate([u for u, _, _ in usable])
    y_all = np.concatenate([y[warmup:] for _, y, _ in usable])
    if np.ptp(u_all) == 0 or np.ptp(y_all) == 0:
        raise ValueError("the input or the response has no variation over the fitted samples")
    u_mean, y_mean = float(np.mean(u_all)), float(np.mean(y_all))
    has_z = usable[0][2] is not None
    z_mean = np.mean(np.vstack([z[warmup:] for _, _, z in usable]), axis=0) if has_z else None
    runs = [(u - u_mean, np.ascontiguousarray(np.vstack(
        [y[warmup:] - y_mean] + ([] if z is None else list((z[warmup:] - z_mean).T)))))
        for u, y, z in usable]
    gram = sum(_gram(rows) for _, rows in runs)
    if not has_z:
        return _Prepared(runs, y_all.size, float(gram[0, 0]), None, None)
    energy = sum(float(np.sum(z[warmup:] ** 2)) for _, _, z in usable)
    whiten = _whitening(gram[1:, 1:], energy)
    return _Prepared(runs, y_all.size, float(gram[0, 0]), whiten, gram[1:, 0] @ whiten)


def fit_first_order_plant_runs(
    runs: Sequence[tuple[np.ndarray, ...]],
    dt_s: float,
    delays_s: Sequence[float],
    taus_s: Sequence[float],
) -> PlantFit:
    """Fit one plant to several runs of (u, y) or (u, y, instruments) on one dt_s grid.

    Each run is filtered and delayed on its own, so no delay reaches across a
    gap between runs; the sums of all runs are then added. Every grid point is
    scored on the same response samples: those of each run after its first
    longest-delay samples. See fit_first_order_plant for the estimators.
    """
    delays, taus = np.asarray(delays_s, dtype=float), np.asarray(taus_s, dtype=float)
    if not dt_s > 0 or delays.size == 0 or taus.size == 0 or np.any(delays < 0):
        raise ValueError("need dt_s > 0 and non-empty grids of non-negative delays")
    grid = _delay_grid(dt_s, delays)
    prepared = _prepare(_usable_runs(runs, grid.warmup), grid.warmup)
    scores = np.empty((3, taus.size, delays.size))
    for i, tau in enumerate(taus):
        sums = sum(_run_sums(u, rows, dt_s, tau, grid) for u, rows in prepared.runs)
        scores[:, i] = _scores(sums, prepared)
    r2, gain, first = scores
    if not np.any(np.isfinite(r2)):
        raise ValueError("the input has no variation over the fitted samples"
                         if prepared.whiten is None else
                         "the instruments carry no signal for this axis")
    i, j = np.unravel_index(int(np.argmax(r2)), r2.shape)
    return PlantFit(gain=float(gain[i, j]), delay_s=float(delays[j]), tau_s=float(taus[i]),
                    r2=float(r2[i, j]), first_stage_r2=float(first[i, j]),
                    reduced_form_r2=prepared.reduced_form_r2)


def fit_first_order_plant(
    u: np.ndarray,
    y: np.ndarray,
    dt_s: float,
    delays_s: Sequence[float],
    taus_s: Sequence[float],
    instruments: np.ndarray | None = None,
) -> PlantFit:
    """Fit y = gain * x + offset, x = u low-passed by tau and delayed; keep the best grid point.

    u, y (and the rows of instruments) are uniformly sampled every dt_s; x is
    first_order_lowpass(u, tau) delayed by the grid delay (linear interpolation
    between samples).

    Without instruments the gain is least squares and the point with the highest
    R^2 is kept. With an (n, k) instrument matrix Z (signals correlated with u
    but not with the noise in y) the gain is two-stage least squares: x is
    projected on Z, y regressed on that projection, gain = x'Py / x'Px. The
    point kept is the one with the highest R^2 projected on Z, 1 - e'Pe / y'Py.
    Z is the same at every grid point, so every point is scored on the same
    projected response. first_stage_r2 is the R^2 of x on Z at that point.
    """
    run = (u, y) if instruments is None else (u, y, instruments)
    return fit_first_order_plant_runs([run], dt_s, delays_s, taus_s)


# ---------------------------------------------------------------------------
# Filter group delays
# ---------------------------------------------------------------------------


def butterworth2_group_delay_s(cutoff_hz: float) -> float:
    """Low-frequency group delay of a second-order Butterworth low-pass: sqrt(2) / (2 pi fc).

    PX4's LowPassFilter2p; 0 for a cutoff <= 0, which disables it.
    """
    if not cutoff_hz > 0:
        return 0.0
    return math.sqrt(2.0) / (2.0 * math.pi * cutoff_hz)


def first_order_group_delay_s(cutoff_hz: float) -> float:
    """Low-frequency group delay of a first-order low-pass: its time constant 1 / (2 pi fc).

    PX4's AlphaFilter set by cutoff frequency; 0 for a cutoff <= 0, which disables it.
    """
    if not cutoff_hz > 0:
        return 0.0
    return 1.0 / (2.0 * math.pi * cutoff_hz)
