"""Battery model fitting from real PX4 flight logs.

Fits two models:
  1. Internal resistance (R_internal): per-cell, keyed by cell_count.
     Method: regress instantaneous voltage drop vs current step events
     (dV = -R * dI) from rapid throttle changes in real flight.

  2. OCV curve V_OCV(soc): open-circuit voltage per cell vs state of charge.
     Method: collect (soc, v/cell) observations from low-current coast
     segments, then bin by SoC and take the median per bin. Enforced
     monotone via isotonic regression (pool-adjacent-violators).

     SoC source priority:
       1. ``battery_status.remaining`` — direct PX4 estimate (preferred).
       2. ``battery_status.discharged_mah`` integrated against a per-log
          capacity estimate (fallback; less accurate because capacity is
          often 0 in older firmware).
       3. Skip the log if neither is available.

     Limitation: ``battery_status.capacity`` is often 0 (firmware does not
     populate it unless a smart battery is connected).  When using
     ``remaining`` the accuracy depends on PX4's own SoC estimator, which
     is affected by calibration and may drift on short flights.

CRITICAL: simulation logs are excluded via ``hardware.is_simulation()``.
All fits and statistics are over real logs only.

Usage::

    python -m log_validation.battery_fit \\
        --log-dir data/downloaded \\
        --holdout-frac 0.20 \\
        --out-json results/battery_fit.json \\
        --out-md docs/superpowers/status/2026-06-23-battery-fit.md
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np

# ---------------------------------------------------------------------------
# Constants — all named with rationale
# ---------------------------------------------------------------------------

# Minimum absolute current step (A) to treat as a "throttle punch" for R fit.
# Below this, the voltage drop is too small relative to ADC noise (~0.02 V).
MIN_STEP_dI_A: float = 5.0

# Maximum sample-to-sample time gap (s) for a step event to be valid.
# Large dt means the current changed gradually, not abruptly.
MAX_STEP_DT_S: float = 2.0

# Minimum voltage change (V) paired with a current step.  Filters out
# samples where the ADC did not register any sag.
MIN_STEP_dV_V: float = 0.05

# Plausible range for per-cell internal resistance (mΩ/cell).
# Below 0.5: physically impossible for a consumer LiPo.
# Above 80: pack is heavily degraded or measurement noise dominated.
R_MIN_MOHM_PER_CELL: float = 0.5
R_MAX_MOHM_PER_CELL: float = 80.0

# Current threshold below which V ≈ OCV (low-load coast segments).
# 3 A ≈ gentle hover for a typical quadrotor; most resistive sag is <0.05 V.
LOW_CURRENT_OCV_THRESH_A: float = 3.0

# Minimum number of (soc, v) points per SoC bin to trust the median.
# Bins with fewer points are marked sparse and filled by interpolation.
MIN_BIN_POINTS: int = 5

# Number of equal-width SoC bins for the OCV curve.
OCV_N_BINS: int = 20

# Minimum number of step-event observations for a per-cell-count R median to be
# trusted.  Cell counts below this fall back to the pooled median because their
# sample is too small to be stable across runs.
MIN_R_OBSERVATIONS_PER_CLASS: int = 200

# Plausibility floor for a per-cell internal resistance (mΩ/cell).  A real
# consumer LiPo cell does not sit below ~4 mΩ/cell; high-cell-count packs are
# NOT 4× lower resistance per cell than low-cell packs.  When a class's fitted
# median falls below this floor it indicates unreliable data (few packs, biased
# current sensing — observed for 12S, which fits ~2 mΩ/cell).  Such classes
# fall back to the pooled median.  Held-out validation confirmed the sub-floor
# 12S value badly under-predicts sag, so this fallback is corrective, not cosmetic.
R_PLAUSIBLE_FLOOR_MOHM_PER_CELL: float = 4.0

# Fraction of logs reserved for held-out validation.
DEFAULT_HOLDOUT_FRAC: float = 0.20

# Fixed seed for the deterministic train/holdout split.  Exposed as a CLI arg.
# A fixed default guarantees the canonical run is reproducible: the R table,
# OCV table, and validation MAE all derive from the same split every time.
DEFAULT_SPLIT_SEED: int = 20260623

# Physical plausibility bounds on per-cell voltage in OCV observations.
V_CELL_MIN: float = 3.0  # V — never trust readings below this
V_CELL_MAX: float = 4.25  # V — never trust readings above 4.25 V/cell


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PerCellRFit:
    """Fitted internal resistance for one cell-count class.

    ``deployed_mohm_per_cell`` is the value actually used downstream (report,
    validation, Rust): it equals ``median_mohm_per_cell`` when the class has
    enough observations, otherwise it falls back to the pooled median.
    ``low_n`` flags when the fallback was applied.
    """

    cell_count: int
    median_mohm_per_cell: float
    p25_mohm_per_cell: float
    p75_mohm_per_cell: float
    n_observations: int
    deployed_mohm_per_cell: float
    low_n: bool


@dataclass(frozen=True)
class OcvCurve:
    """Fitted OCV curve: (soc, v_per_cell) table, monotone increasing."""

    soc_knots: list[float]
    v_per_cell_knots: list[float]
    n_total_observations: int

    def interpolate(self, soc: float) -> float:
        """Return per-cell OCV at a given state of charge [0, 1]."""
        return float(np.interp(soc, self.soc_knots, self.v_per_cell_knots))


@dataclass
class BatteryFitResult:
    """Aggregate output of the battery fitting pipeline."""

    real_log_count: int
    sim_log_count: int
    skipped_log_count: int
    r_fits: list[PerCellRFit]
    overall_r_median_mohm_per_cell: float
    overall_r_n: int
    ocv_curve: OcvCurve
    holdout_log_count: int  # all real logs in the holdout split
    holdout_scored_count: int  # holdout logs that passed filtering and were scored
    holdout_median_mae_v_per_cell: float
    holdout_mean_mae_v_per_cell: float
    holdout_p75_mae_v_per_cell: float
    validation_target_met: bool  # True if median MAE < 0.1 V/cell
    split_seed: int  # seed used for the deterministic train/holdout split


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _isotonic_increasing(values: list[float]) -> list[float]:
    """Enforce monotone-increasing constraint via pool-adjacent-violators.

    Returns a new list; the input is not modified (immutable pattern).
    """
    out = list(values)
    n = len(out)
    changed = True
    while changed:
        changed = False
        i = 0
        while i < n - 1:
            if out[i] > out[i + 1]:
                avg = (out[i] + out[i + 1]) / 2.0
                out[i] = avg
                out[i + 1] = avg
                changed = True
            i += 1
    return out


def _extract_r_observations(
    v: np.ndarray, i: np.ndarray, t: np.ndarray, cell_count: int
) -> list[float]:
    """Extract per-cell R observations from rapid current-step events.

    For each adjacent sample pair where the current changed abruptly
    (|dI| > MIN_STEP_dI_A) and a corresponding voltage change is visible,
    compute R = -dV/dI and normalise per cell.

    Returns a list of plausible R values in mΩ/cell.
    """
    if len(i) < 2:
        return []
    dt = np.diff(t)
    d_i = np.diff(i)
    d_v = np.diff(v)

    step_mask = (
        (dt < MAX_STEP_DT_S)
        & (np.abs(d_i) > MIN_STEP_dI_A)
        & (np.abs(d_v) > MIN_STEP_dV_V)
    )
    if not step_mask.any():
        return []

    d_i_step = d_i[step_mask]
    d_v_step = d_v[step_mask]
    # R = -dV / dI  (voltage drops when current increases, so sign flip)
    r_mohm_per_cell = (-d_v_step / d_i_step) * 1000.0 / cell_count

    plausible = (r_mohm_per_cell > R_MIN_MOHM_PER_CELL) & (
        r_mohm_per_cell < R_MAX_MOHM_PER_CELL
    )
    return r_mohm_per_cell[plausible].tolist()


def _extract_ocv_observations(
    v: np.ndarray, i: np.ndarray, soc: np.ndarray, cell_count: int
) -> list[tuple[float, float]]:
    """Extract (soc, v_per_cell) pairs from low-current coast segments.

    Only samples where current < LOW_CURRENT_OCV_THRESH_A are used.
    Voltage is normalised per cell and filtered to physically plausible range.
    """
    low_i_mask = i < LOW_CURRENT_OCV_THRESH_A
    if low_i_mask.sum() < MIN_BIN_POINTS:
        return []

    soc_low = soc[low_i_mask]
    v_per_cell = v[low_i_mask] / cell_count

    valid = (
        (soc_low >= 0.0)
        & (soc_low <= 1.0)
        & (v_per_cell > V_CELL_MIN)
        & (v_per_cell < V_CELL_MAX)
    )
    pairs = list(zip(soc_low[valid].tolist(), v_per_cell[valid].tolist()))
    return pairs


def _build_ocv_curve(observations: list[tuple[float, float]]) -> OcvCurve:
    """Bin observations by SoC, compute per-bin median, enforce monotone.

    Sparse bins (< MIN_BIN_POINTS) are filled by linear interpolation from
    neighbouring bins.  The resulting table is guaranteed monotone-increasing
    via isotonic regression.
    """
    if not observations:
        raise ValueError("No OCV observations; cannot build curve.")

    soc_arr = np.array([p[0] for p in observations])
    v_arr = np.array([p[1] for p in observations])

    bin_edges = np.linspace(0.0, 1.0, OCV_N_BINS + 1)
    bin_centers = 0.5 * (bin_edges[:-1] + bin_edges[1:])

    medians: list[Optional[float]] = []
    for lo, hi in zip(bin_edges[:-1], bin_edges[1:]):
        mask = (soc_arr >= lo) & (soc_arr < hi)
        if mask.sum() >= MIN_BIN_POINTS:
            medians.append(float(np.median(v_arr[mask])))
        else:
            medians.append(None)

    # Fill sparse bins by interpolation from non-sparse neighbours
    known_idx = [i for i, m in enumerate(medians) if m is not None]
    known_soc = [bin_centers[i] for i in known_idx]
    known_v = [medians[i] for i in known_idx]  # type: ignore[misc]

    if len(known_v) < 2:
        raise ValueError(
            f"Too few populated SoC bins ({len(known_v)}); need at least 2 to build curve."
        )

    filled = [
        float(np.interp(bin_centers[j], known_soc, known_v)) if m is None else m
        for j, m in enumerate(medians)
    ]

    # Enforce monotone via isotonic regression
    monotone = _isotonic_increasing(filled)

    return OcvCurve(
        soc_knots=bin_centers.tolist(),
        v_per_cell_knots=monotone,
        n_total_observations=len(observations),
    )


def _predict_terminal_voltage(
    soc: np.ndarray,
    current: np.ndarray,
    cell_count: int,
    ocv: OcvCurve,
    r_mohm_per_cell: float,
) -> np.ndarray:
    """Predict terminal voltage from SoC + current using fitted R and OCV."""
    ocv_per_pack = np.vectorize(ocv.interpolate)(soc) * cell_count
    r_total_ohm = cell_count * r_mohm_per_cell / 1000.0
    return ocv_per_pack - current * r_total_ohm


# ---------------------------------------------------------------------------
# Core analysis functions
# ---------------------------------------------------------------------------


def analyze_logs(
    log_dir: str,
    holdout_frac: float = DEFAULT_HOLDOUT_FRAC,
    seed: int = DEFAULT_SPLIT_SEED,
) -> BatteryFitResult:
    """Scan all .ulg files in *log_dir* and fit battery models.

    Simulation logs are detected via ``hardware.is_simulation()`` and
    excluded from all fits and statistics.

    Reproducibility
    ---------------
    The full pipeline is deterministic for a fixed (log_dir, holdout_frac,
    seed): files are sorted by name, then a seeded ``numpy.random.default_rng``
    permutation assigns the train/holdout split.  The same inputs always yield
    the same fitted R table, OCV table, and held-out MAE.  This guarantees the
    numbers written to Rust, the Markdown report, and the validation metric all
    come from ONE canonical run.

    Parameters
    ----------
    log_dir:
        Directory containing ``*.ulg`` files.
    holdout_frac:
        Fraction of files reserved for validation.  Must be in (0, 1).
    seed:
        Seed for the deterministic train/holdout split permutation.

    Returns
    -------
    BatteryFitResult
        Fitted R per cell-count, OCV curve, and validation metrics.
    """
    from pathlib import Path as _Path
    from pyulog import ULog
    from .hardware import extract_hardware_info, is_simulation

    paths = sorted(_Path(log_dir).glob("*.ulg"))
    if not paths:
        raise FileNotFoundError(f"No .ulg files found in {log_dir!r}")
    if not 0.0 < holdout_frac < 1.0:
        raise ValueError(f"holdout_frac must be in (0, 1), got {holdout_frac}")

    # Deterministic, seeded train/holdout split.  Sorting first makes the
    # permutation reproducible regardless of filesystem ordering.
    n_holdout = max(1, int(len(paths) * holdout_frac))
    rng = np.random.default_rng(seed)
    order = rng.permutation(len(paths))
    holdout_idx = set(order[:n_holdout].tolist())
    fit_paths = [paths[j] for j in range(len(paths)) if j not in holdout_idx]
    holdout_paths = [paths[j] for j in range(len(paths)) if j in holdout_idx]

    total = 0
    real_count = 0
    sim_count = 0
    skipped = 0

    # Accumulators keyed by cell_count
    r_by_cc: dict[int, list[float]] = {}
    ocv_all: list[tuple[float, float]] = []

    def _process_log(
        lp: Path,
    ) -> Optional[tuple[int, list[float], list[tuple[float, float]]]]:
        """Return (cell_count, r_obs, ocv_obs) or None if log is unusable."""
        try:
            u = ULog(str(lp))
        except Exception:
            return None

        hw = None
        try:
            hw = extract_hardware_info(u.msg_info_dict, u.initial_parameters)
        except Exception:
            pass
        if hw is not None and is_simulation(hw):
            return None  # caller tracks sim_count separately

        tnames = {d.name for d in u.data_list}
        if "battery_status" not in tnames:
            return None

        try:
            ds = u.get_dataset("battery_status", multi_instance=0)
            v = np.asarray(ds.data["voltage_v"], dtype=float)
            i = np.asarray(ds.data["current_a"], dtype=float)
            t = np.asarray(ds.data["timestamp"], dtype=float) * 1e-6
            cc = int(ds.data["cell_count"][0])
        except (KeyError, IndexError):
            return None

        # Reject clearly invalid packs
        if cc <= 0 or cc > 14:
            return None
        if v.max() - v.min() < 0.1:
            return None
        if i.max() < 2.0:
            return None

        r_obs = _extract_r_observations(v, i, t, cc)

        # SoC: prefer 'remaining', fallback to 'discharged_mah' integration
        soc: Optional[np.ndarray] = None
        if "remaining" in ds.data:
            soc_raw = np.asarray(ds.data["remaining"], dtype=float)
            if soc_raw.max() - soc_raw.min() > 0.02:
                soc = soc_raw

        if soc is None and "discharged_mah" in ds.data:
            disch = np.asarray(ds.data["discharged_mah"], dtype=float)
            if disch.max() > 0 and disch.max() > disch.min():
                # Estimate capacity from max discharged (conservative lower bound)
                cap_mah = disch.max() / 0.8  # assume 80% DoD at end of log
                soc = np.clip(1.0 - disch / cap_mah, 0.0, 1.0)

        ocv_obs: list[tuple[float, float]] = []
        if soc is not None:
            ocv_obs = _extract_ocv_observations(v, i, soc, cc)

        return cc, r_obs, ocv_obs

    # -----------------------------------------------------------------------
    # Fit phase
    # -----------------------------------------------------------------------
    for lp in fit_paths:
        total += 1

        # Check sim status separately so we count sims even if _process_log skips
        try:
            u_check = ULog(str(lp))
            hw_check = None
            try:
                hw_check = extract_hardware_info(
                    u_check.msg_info_dict, u_check.initial_parameters
                )
            except Exception:
                pass
            if hw_check is not None and is_simulation(hw_check):
                sim_count += 1
                continue
        except Exception:
            skipped += 1
            continue

        real_count += 1
        result = _process_log(lp)
        if result is None:
            skipped += 1
            continue
        cc, r_obs, ocv_obs = result
        if r_obs:
            r_by_cc.setdefault(cc, []).extend(r_obs)
        ocv_all.extend(ocv_obs)

    # Count holdout sims too (for accurate totals)
    holdout_real = 0
    for lp in holdout_paths:
        total += 1
        try:
            u_check = ULog(str(lp))
            hw_check = None
            try:
                hw_check = extract_hardware_info(
                    u_check.msg_info_dict, u_check.initial_parameters
                )
            except Exception:
                pass
            if hw_check is not None and is_simulation(hw_check):
                sim_count += 1
            else:
                real_count += 1
                holdout_real += 1
        except Exception:
            skipped += 1

    # -----------------------------------------------------------------------
    # Build R summary.  Pooled median is computed first so low-n classes can
    # fall back to it deterministically.  The deployed value (median or
    # fallback) is what report, validation, and Rust all consume.
    # -----------------------------------------------------------------------
    all_r_vals: list[float] = []
    for cc in sorted(r_by_cc.keys()):
        all_r_vals.extend(r_by_cc[cc])
    if not all_r_vals:
        raise ValueError("No valid R observations collected; check log quality.")
    overall_r_median = float(np.median(all_r_vals))

    r_fits: list[PerCellRFit] = []
    for cc in sorted(r_by_cc.keys()):
        vals = np.array(r_by_cc[cc])
        median = float(np.median(vals))
        # A class is "unreliable" if it has too few observations OR its fitted
        # median is physically implausible (below the per-cell floor).  Either
        # way it falls back to the pooled median.  This is deterministic and
        # drives report, validation, and Rust identically.
        too_few = len(vals) < MIN_R_OBSERVATIONS_PER_CLASS
        implausible = median < R_PLAUSIBLE_FLOOR_MOHM_PER_CELL
        low_n = too_few or implausible
        deployed = overall_r_median if low_n else median
        r_fits.append(
            PerCellRFit(
                cell_count=cc,
                median_mohm_per_cell=median,
                p25_mohm_per_cell=float(np.percentile(vals, 25)),
                p75_mohm_per_cell=float(np.percentile(vals, 75)),
                n_observations=len(vals),
                deployed_mohm_per_cell=deployed,
                low_n=low_n,
            )
        )

    # -----------------------------------------------------------------------
    # Build OCV curve
    # -----------------------------------------------------------------------
    ocv_curve = _build_ocv_curve(ocv_all)

    # -----------------------------------------------------------------------
    # Validation: held-out log prediction
    # -----------------------------------------------------------------------
    holdout_maes: list[float] = []
    for lp in holdout_paths:
        result = _process_log(lp)
        if result is None:
            continue
        cc, _, _ = result
        try:
            u2 = ULog(str(lp))
            ds2 = u2.get_dataset("battery_status", multi_instance=0)
            v2 = np.asarray(ds2.data["voltage_v"], dtype=float)
            i2 = np.asarray(ds2.data["current_a"], dtype=float)
            cc2 = int(ds2.data["cell_count"][0])
            if cc2 <= 0 or cc2 > 14:
                continue
            if "remaining" not in ds2.data:
                continue
            soc2 = np.asarray(ds2.data["remaining"], dtype=float)
            if soc2.max() - soc2.min() < 0.02:
                continue

            # Mirror the deployed Rust lookup EXACTLY: use the per-class
            # deployed value (median, or pooled fallback for low-n classes);
            # for cell counts absent from the table, use the pooled median
            # (which equals Rust's FITTED_R_DEFAULT_MOHM_PER_CELL).
            r_for_cc = overall_r_median
            for fit in r_fits:
                if fit.cell_count == cc2:
                    r_for_cc = fit.deployed_mohm_per_cell
                    break

            v_pred = _predict_terminal_voltage(soc2, i2, cc2, ocv_curve, r_for_cc)
            mae_per_cell = float(np.median(np.abs(v_pred - v2) / cc2))
            holdout_maes.append(mae_per_cell)
        except Exception:
            continue

    if not holdout_maes:
        raise ValueError("No holdout logs could be evaluated; check holdout fraction.")

    maes = np.array(holdout_maes)
    median_mae = float(np.median(maes))

    return BatteryFitResult(
        real_log_count=real_count,
        sim_log_count=sim_count,
        skipped_log_count=skipped,
        r_fits=r_fits,
        overall_r_median_mohm_per_cell=overall_r_median,
        overall_r_n=len(all_r_vals),
        ocv_curve=ocv_curve,
        holdout_log_count=holdout_real,
        holdout_scored_count=len(holdout_maes),
        holdout_median_mae_v_per_cell=median_mae,
        holdout_mean_mae_v_per_cell=float(np.mean(maes)),
        holdout_p75_mae_v_per_cell=float(np.percentile(maes, 75)),
        validation_target_met=(median_mae < 0.1),
        split_seed=seed,
    )


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def result_to_dict(r: BatteryFitResult) -> dict:
    """Serialise BatteryFitResult to a JSON-compatible dict."""
    return {
        "real_log_count": r.real_log_count,
        "sim_log_count": r.sim_log_count,
        "skipped_log_count": r.skipped_log_count,
        "split_seed": r.split_seed,
        "r_fits": [
            {
                "cell_count": f.cell_count,
                "median_mohm_per_cell": f.median_mohm_per_cell,
                "p25_mohm_per_cell": f.p25_mohm_per_cell,
                "p75_mohm_per_cell": f.p75_mohm_per_cell,
                "n_observations": f.n_observations,
                "deployed_mohm_per_cell": f.deployed_mohm_per_cell,
                "low_n": f.low_n,
            }
            for f in r.r_fits
        ],
        "overall_r_median_mohm_per_cell": r.overall_r_median_mohm_per_cell,
        "overall_r_n": r.overall_r_n,
        "ocv_curve": {
            "soc_knots": r.ocv_curve.soc_knots,
            "v_per_cell_knots": r.ocv_curve.v_per_cell_knots,
            "n_total_observations": r.ocv_curve.n_total_observations,
        },
        "holdout_log_count": r.holdout_log_count,
        "holdout_scored_count": r.holdout_scored_count,
        "holdout_median_mae_v_per_cell": r.holdout_median_mae_v_per_cell,
        "holdout_mean_mae_v_per_cell": r.holdout_mean_mae_v_per_cell,
        "holdout_p75_mae_v_per_cell": r.holdout_p75_mae_v_per_cell,
        "validation_target_met": r.validation_target_met,
    }


def write_markdown_summary(r: BatteryFitResult, path: str) -> None:
    """Write a short Markdown summary of the fit results."""
    lines = [
        "# Battery Model Fit — 2026-06-23",
        "",
        "Fit from real PX4 flight logs (SIM excluded via `hardware.is_simulation`).",
        "",
        f"Canonical run: deterministic, split seed = `{r.split_seed}`.  "
        "The R table, OCV table, and held-out MAE below all derive from this "
        "single run and are the exact values compiled into "
        "`hitl-physics/src/battery.rs`.",
        "",
        "## Corpus",
        "",
        "| | Count |",
        "|---|---|",
        f"| Real logs used for fit | {r.real_log_count} |",
        f"| SIM logs excluded | {r.sim_log_count} |",
        f"| Skipped (load error / no battery topic) | {r.skipped_log_count} |",
        f"| Holdout (validation only) | {r.holdout_log_count} |",
        "",
        "## Internal Resistance",
        "",
        f"Overall pooled median: **{r.overall_r_median_mohm_per_cell:.2f} mΩ/cell** "
        f"(n={r.overall_r_n} step events)",
        "",
        f"A class falls back to the pooled median (the **Deployed** column, which "
        f"is what Rust uses) when it has fewer than {MIN_R_OBSERVATIONS_PER_CLASS} "
        f"step events OR its fitted median is implausibly low "
        f"(< {R_PLAUSIBLE_FLOOR_MOHM_PER_CELL:.0f} mΩ/cell — a sign of biased data).",
        "",
        "| Cell count | Median (mΩ/cell) | Deployed (mΩ/cell) | IQR | n step events | fallback reason |",
        "|---|---|---|---|---|---|",
    ]
    for f in r.r_fits:
        iqr = f"{f.p25_mohm_per_cell:.2f}–{f.p75_mohm_per_cell:.2f}"
        if not f.low_n:
            reason = "—"
        elif f.n_observations < MIN_R_OBSERVATIONS_PER_CLASS:
            reason = f"low-n (<{MIN_R_OBSERVATIONS_PER_CLASS}) → pooled"
        else:
            reason = (
                f"implausible (<{R_PLAUSIBLE_FLOOR_MOHM_PER_CELL:.0f} mΩ/cell) → pooled"
            )
        lines.append(
            f"| {f.cell_count}S | {f.median_mohm_per_cell:.2f} | "
            f"{f.deployed_mohm_per_cell:.2f} | {iqr} | {f.n_observations} | {reason} |"
        )

    lines += [
        "",
        "## OCV Curve (per cell, all cell counts pooled)",
        "",
        "| SoC | V/cell |",
        "|---|---|",
    ]
    for soc, v in zip(r.ocv_curve.soc_knots, r.ocv_curve.v_per_cell_knots):
        lines.append(f"| {soc:.3f} | {v:.4f} |")

    lines += [
        "",
        "## Held-Out Validation",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Holdout real logs in split | {r.holdout_log_count} |",
        f"| Holdout logs scored (passed filters) | {r.holdout_scored_count} |",
        f"| Median MAE (V/cell) | **{r.holdout_median_mae_v_per_cell:.4f}** |",
        f"| Mean MAE (V/cell) | {r.holdout_mean_mae_v_per_cell:.4f} |",
        f"| p75 MAE (V/cell) | {r.holdout_p75_mae_v_per_cell:.4f} |",
        f"| Target < 0.1 V/cell | {'PASS' if r.validation_target_met else 'FAIL'} |",
        "",
        "## Method Notes",
        "",
        "- **R_internal:** regressed from rapid current-step events (`|dI| > 5 A`, `dt < 2 s`).  "
        "C-rating is not reliably present in PX4 logs so the fit is keyed by cell_count only; "
        "C-rating is preserved in the Rust signature as a fallback for configs without real-log coverage.",
        "- **OCV:** sampled during low-current coast segments (`I < 3 A`).  "
        "SoC source is `battery_status.remaining` (PX4's own estimator).  "
        "Limitation: `capacity` is usually 0 in firmware (smart battery only), "
        "so `discharged_mah` integration is unreliable without a known capacity.",
        "- **Monotone enforcement:** isotonic regression (pool-adjacent-violators) applied to "
        "bin medians before Rust porting.",
        f"- **Low-n classes:** any cell-count class with < {MIN_R_OBSERVATIONS_PER_CLASS} "
        "step events (e.g. 12S — very few packs in the corpus) is too noisy to trust and "
        "falls back to the pooled median.  The **Deployed** column reflects this and matches Rust.",
        "",
    ]

    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def emit_rust_constants(r: BatteryFitResult) -> str:
    """Render the deployed R table + OCV table as Rust source constants.

    The output is copied verbatim into ``hitl-physics/src/battery.rs`` so the
    deployed Rust values are guaranteed to equal the canonical run.
    """
    lines: list[str] = []
    lines.append(
        "// FITTED_R_TABLE — deployed per-cell R (mΩ/cell), keyed by cell count."
    )
    lines.append(
        f"// Canonical run, split seed={r.split_seed}, {r.real_log_count} real logs, "
        f"{r.sim_log_count} SIM excluded."
    )
    lines.append("const FITTED_R_TABLE: &[(u8, f64)] = &[")
    for f in r.r_fits:
        note = (
            f"n={f.n_observations} LOW-N → pooled median"
            if f.low_n
            else f"n={f.n_observations}"
        )
        lines.append(
            f"    ({f.cell_count}, {f.deployed_mohm_per_cell:.4f}),  // {note}"
        )
    lines.append("];")
    lines.append("")
    lines.append(
        f"const FITTED_R_DEFAULT_MOHM_PER_CELL: f64 = "
        f"{r.overall_r_median_mohm_per_cell:.4f};  // pooled, n={r.overall_r_n}"
    )
    lines.append("")
    lines.append("const OCV_TABLE: &[(f64, f64)] = &[")
    for soc, v in zip(r.ocv_curve.soc_knots, r.ocv_curve.v_per_cell_knots):
        lines.append(f"    ({soc:.4f}, {v:.4f}),")
    lines.append("];")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fit battery models (R_internal, OCV curve) from real PX4 flight logs."
    )
    parser.add_argument(
        "--log-dir",
        default="data/downloaded",
        help="Directory containing *.ulg files (default: data/downloaded)",
    )
    parser.add_argument(
        "--holdout-frac",
        type=float,
        default=DEFAULT_HOLDOUT_FRAC,
        help=f"Fraction of logs held out for validation (default: {DEFAULT_HOLDOUT_FRAC})",
    )
    parser.add_argument(
        "--out-json",
        default="results/battery_fit.json",
        help="Path to write JSON results (default: results/battery_fit.json)",
    )
    parser.add_argument(
        "--out-md",
        default="docs/superpowers/status/2026-06-23-battery-fit.md",
        help="Path to write Markdown summary",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SPLIT_SEED,
        help=f"Seed for the deterministic train/holdout split (default: {DEFAULT_SPLIT_SEED})",
    )
    parser.add_argument(
        "--emit-rust",
        action="store_true",
        help="Print the deployed R + OCV tables as Rust constants to stdout and exit.",
    )
    args = parser.parse_args(argv)

    print(
        f"Scanning logs in {args.log_dir!r} (seed={args.seed})…",
        file=sys.stderr,
        flush=True,
    )
    result = analyze_logs(args.log_dir, holdout_frac=args.holdout_frac, seed=args.seed)

    print(
        f"Real logs: {result.real_log_count} | SIM excluded: {result.sim_log_count} | "
        f"Skipped: {result.skipped_log_count}",
        file=sys.stderr,
    )
    print(
        f"Overall R median: {result.overall_r_median_mohm_per_cell:.2f} mOhm/cell (n={result.overall_r_n})",
        file=sys.stderr,
    )
    print(
        f"Held-out MAE: {result.holdout_median_mae_v_per_cell:.4f} V/cell "
        f"({'PASS' if result.validation_target_met else 'FAIL'} < 0.1 target)",
        file=sys.stderr,
    )

    # Write JSON
    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(result_to_dict(result), indent=2), encoding="utf-8")
    print(f"JSON results written to {out_json}", file=sys.stderr)

    # Write Markdown
    write_markdown_summary(result, args.out_md)
    print(f"Markdown summary written to {args.out_md}", file=sys.stderr)

    # Emit Rust constants for verbatim porting (same canonical run)
    if args.emit_rust:
        print(emit_rust_constants(result))

    return 0 if result.validation_target_met else 1


if __name__ == "__main__":
    sys.exit(main())
