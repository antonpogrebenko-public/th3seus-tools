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

Two steps, so logs can be processed in batches: the extract step reads each
selected log once and saves one compressed numpy file per usable real log
(``extract_dir``); the fit step splits, fits and scores over those files
(``fit_extracted``, ``fit_obs_dir``).  ``analyze_logs`` runs both over one
folder through a temporary directory and gives the same numbers.

Usage::

    python -m log_validation.battery_fit \\
        --log-dir data/downloaded \\
        --holdout-frac 0.20 \\
        --out-json results/battery_fit.json \\
        --out-md docs/superpowers/status/2026-06-23-battery-fit.md

    # the same in two steps (each batch extracted into one folder, then fit)
    python -m log_validation.battery_fit --log-dir BATCH --extract-to OBS --manifest M.csv
    python -m log_validation.battery_fit --obs-dir OBS [OBS2 ...] --manifest M.csv --out-json ... --out-md ...
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import numpy as np

# The extract step, the result types and the outputs live in their own modules;
# their public names are re-exported so `from log_validation.battery_fit import ...` works.
from .battery_extract import (  # noqa: F401
    BATTERY_TOPIC, MAX_CELL_COUNT, MIN_PEAK_CURRENT_A, MIN_VOLTAGE_RANGE_V, NPZ_SUFFIX,
    STATUS_LOAD_ERROR, STATUS_OK, STATUS_SIM, STATUS_UNUSABLE, STATUS_WORKER_DIED, STATUSES,
    LogBatteryData, extract_dir, extract_log, load_battery_topics, load_extracted,
    STATUS_NO_MANIFEST_ROW, load_named, read_obs_dirs, read_statuses, save_extracted,
)
from .battery_model import (  # noqa: F401
    MIN_R_OBSERVATIONS_PER_CLASS, R_PLAUSIBLE_FLOOR_MOHM_PER_CELL, BatteryFitResult, OcvCurve,
    PerCellRFit,
)
from .battery_output import emit_rust_constants, result_to_dict, write_markdown_summary
from .manifest import (
    DEFAULT_MAX_PER_VEHICLE, ManifestError, add_selection_args, cap_per_vehicle,
    check_max_per_vehicle, read_manifest,
)
from .outputs import PrivateDataError, forbidden_strings, write_json

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
# Fit step constants (the values of the one-pass analysis, named)
# ---------------------------------------------------------------------------

# battery_status.remaining serves as SoC only when it moves: by more than this
# for the OCV fit, by at least this for holdout scoring (as before).
SOC_MIN_RANGE: float = 0.02

# Depth of discharge assumed at the end of a log when SoC comes from
# discharged_mah: the capacity estimate is max(discharged_mah) / this.
ASSUMED_END_DEPTH_OF_DISCHARGE: float = 0.8

# Held-out median MAE below which the fit is reported as a PASS (V/cell).
VALIDATION_TARGET_V_PER_CELL: float = 0.1


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _isotonic_increasing(values: list[float]) -> list[float]:
    """Enforce monotone-increasing constraint via pool-adjacent-violators.

    A stack of (sum, count) blocks: each value is pushed as its own block and
    merged with the previous block while the previous mean is greater, so it
    ends after at most n merges (repeated pairwise averaging could cycle
    forever on values a few ULPs apart).  Each value becomes its block's mean.
    Returns a new list; the input is not modified (immutable pattern).
    """
    blocks: list[tuple[float, int]] = []
    for value in values:
        total, count = float(value), 1
        while blocks and blocks[-1][0] / blocks[-1][1] > total / count:
            prev_total, prev_count = blocks.pop()
            total, count = total + prev_total, count + prev_count
        blocks.append((total, count))
    return [total / count for total, count in blocks for _ in range(count)]


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
# Split
# ---------------------------------------------------------------------------


def _check_holdout_frac(holdout_frac: float) -> None:
    if not 0.0 < holdout_frac < 1.0:
        raise ValueError(f"holdout_frac must be in (0, 1), got {holdout_frac}")


def split_logs(
    log_ids: Sequence[str],
    vehicle_of: Mapping[str, str],
    holdout_frac: float,
    seed: int,
) -> tuple[list[str], list[str]]:
    """Seeded (fit, holdout) split of log IDs; both lists sorted.

    Without a mapping (empty vehicle_of) the unit is the log, exactly as the
    one-pass analysis: sort the IDs, permute with
    ``np.random.default_rng(seed).permutation(n)``, and the first
    ``max(1, int(n * holdout_frac))`` go to holdout.  With a mapping the same
    runs over the sorted distinct vehicle IDs and every log of a holdout
    vehicle goes to holdout, so no vehicle is on both sides.
    """
    _check_holdout_frac(holdout_frac)
    ids = sorted(log_ids)
    if len(set(ids)) != len(ids):
        raise ValueError("a log ID is listed more than once")
    unit_of = _unit_of(ids, vehicle_of)
    units = sorted(set(unit_of.values()))
    n_holdout = max(1, int(len(units) * holdout_frac))
    order = np.random.default_rng(seed).permutation(len(units))
    held = {units[j] for j in order[:n_holdout].tolist()}
    return [i for i in ids if unit_of[i] not in held], [i for i in ids if unit_of[i] in held]


def _unit_of(ids: Sequence[str], vehicle_of: Mapping[str, str]) -> dict[str, str]:
    if not vehicle_of:
        return {i: i for i in ids}
    missing = [i for i in ids if i not in vehicle_of]
    if missing:
        raise ValueError(f"{len(missing)} log ID(s) have no vehicle, e.g. {missing[0]}")
    return {i: vehicle_of[i] for i in ids}


# ---------------------------------------------------------------------------
# Fit step: LogBatteryData -> BatteryFitResult
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _LogObservations:
    log_id: str
    cell_count: int
    r_obs: list[float]
    ocv_obs: list[tuple[float, float]]


def _soc_for_ocv(data: LogBatteryData) -> np.ndarray | None:
    """SoC for the OCV fit: 'remaining' when it moves, else discharged_mah integration."""
    remaining = data.remaining
    if remaining is not None and remaining.max() - remaining.min() > SOC_MIN_RANGE:
        return remaining
    disch = data.discharged_mah
    if disch is not None and disch.max() > 0 and disch.max() > disch.min():
        # Estimate capacity from max discharged (conservative lower bound)
        cap_mah = disch.max() / ASSUMED_END_DEPTH_OF_DISCHARGE
        return np.clip(1.0 - disch / cap_mah, 0.0, 1.0)
    return None


def _observe(data: LogBatteryData) -> _LogObservations:
    soc = _soc_for_ocv(data)
    ocv_obs = (
        [] if soc is None
        else _extract_ocv_observations(data.v, data.i, soc, data.cell_count)
    )
    r_obs = _extract_r_observations(data.v, data.i, data.t_s, data.cell_count)
    return _LogObservations(data.log_id, data.cell_count, r_obs, ocv_obs)


def _vehicle_key(log_id: str, vehicle_of: Mapping[str, str]) -> tuple[str, str]:
    """A log's vehicle; a log without one counts as its own vehicle."""
    return ("vehicle", vehicle_of[log_id]) if log_id in vehicle_of else ("log", log_id)


def _per_cell_fit(
    cell_count: int, r_vals: list[float], n_vehicles: int, overall_r_median: float
) -> PerCellRFit:
    vals = np.array(r_vals)
    median = float(np.median(vals))
    # A class is "unreliable" if it has too few observations OR its fitted
    # median is physically implausible (below the per-cell floor).  Either
    # way it falls back to the pooled median.  This is deterministic and
    # drives report, validation, and Rust identically.
    too_few = len(vals) < MIN_R_OBSERVATIONS_PER_CLASS
    implausible = median < R_PLAUSIBLE_FLOOR_MOHM_PER_CELL
    low_n = too_few or implausible
    return PerCellRFit(
        cell_count=cell_count,
        median_mohm_per_cell=median,
        p25_mohm_per_cell=float(np.percentile(vals, 25)),
        p75_mohm_per_cell=float(np.percentile(vals, 75)),
        n_observations=len(vals),
        deployed_mohm_per_cell=overall_r_median if low_n else median,
        low_n=low_n,
        p10_mohm_per_cell=float(np.percentile(vals, 10)),
        p90_mohm_per_cell=float(np.percentile(vals, 90)),
        n_vehicles=n_vehicles,
    )


def _r_table(
    observations: Sequence[_LogObservations], vehicle_of: Mapping[str, str]
) -> tuple[list[PerCellRFit], float, int]:
    """(rows per cell count, pooled median, pooled n).

    The pooled median is computed first so low-n classes can fall back to it.
    """
    with_r = [o for o in observations if o.r_obs]
    cells = sorted({o.cell_count for o in with_r})
    r_by_cc = {cc: [r for o in with_r if o.cell_count == cc for r in o.r_obs] for cc in cells}
    all_r_vals = [r for cc in cells for r in r_by_cc[cc]]
    if not all_r_vals:
        raise ValueError("No valid R observations collected; check log quality.")
    overall = float(np.median(all_r_vals))
    vehicles = {
        cc: {_vehicle_key(o.log_id, vehicle_of) for o in with_r if o.cell_count == cc}
        for cc in cells
    }
    rows = [_per_cell_fit(cc, r_by_cc[cc], len(vehicles[cc]), overall) for cc in cells]
    return rows, overall, len(all_r_vals)


def _per_log_r(
    observations: Sequence[_LogObservations], vehicle_of: Mapping[str, str]
) -> list[dict]:
    return [
        {
            "log_id": o.log_id,
            "vehicle_id": vehicle_of.get(o.log_id),
            "cell_count": o.cell_count,
            "median_mohm_per_cell": float(np.median(o.r_obs)),
        }
        for o in observations
        if o.r_obs
    ]


def _deployed_r(cell_count: int, r_fits: Sequence[PerCellRFit], overall: float) -> float:
    """Mirror the deployed Rust lookup EXACTLY.

    The per-class deployed value (median, or pooled fallback for low-n
    classes); for cell counts absent from the table, the pooled median
    (which equals Rust's FITTED_R_DEFAULT_MOHM_PER_CELL).
    """
    return next((f.deployed_mohm_per_cell for f in r_fits if f.cell_count == cell_count), overall)


def _holdout_mae(data: LogBatteryData, ocv: OcvCurve, r_mohm_per_cell: float) -> float | None:
    """Median |predicted - measured| V/cell over one log; None when 'remaining' is flat or absent."""
    soc = data.remaining
    if soc is None or soc.max() - soc.min() < SOC_MIN_RANGE:
        return None
    v_pred = _predict_terminal_voltage(soc, data.i, data.cell_count, ocv, r_mohm_per_cell)
    return float(np.median(np.abs(v_pred - data.v) / data.cell_count))


def _statuses_of(datas: Sequence[LogBatteryData], no_data: Mapping[str, str]) -> dict[str, str]:
    ids = [d.log_id for d in datas] + list(no_data)
    repeated = sorted(i for i, n in Counter(ids).items() if n > 1)
    if repeated:
        raise ValueError(f"log ID given more than once: {', '.join(repeated[:5])}")
    unknown = sorted(set(no_data.values()) - set(STATUSES[1:]))
    if unknown:
        raise ValueError(f"unknown extract status: {', '.join(unknown)}")
    return {**{d.log_id: STATUS_OK for d in datas}, **no_data}


def fit_extracted(
    datas: Sequence[LogBatteryData],
    vehicle_of: Mapping[str, str],
    holdout_frac: float = DEFAULT_HOLDOUT_FRAC,
    seed: int = DEFAULT_SPLIT_SEED,
    *,
    no_data: Mapping[str, str] | None = None,
    logs_without_manifest_row: int = 0,
) -> BatteryFitResult:
    """Fit R and OCV on the fit side of the split; score the holdout side.

    ``datas`` are the usable real logs.  ``no_data`` maps every other selected
    log to its extract status ("sim", "unusable", "load_error", "worker_died"): those logs
    keep their place in the split, as every .ulg file had in the one-pass
    analysis, so a split without a manifest equals that analysis's split; they
    give no observation and no score.  ``vehicle_of`` (empty without a
    manifest) makes the split hold out whole vehicles and names the vehicles.
    """
    statuses = _statuses_of(datas, no_data or {})
    fit_ids, holdout_ids = split_logs(list(statuses), vehicle_of, holdout_frac, seed)
    by_id = {d.log_id: d for d in datas}
    observations = [_observe(by_id[i]) for i in fit_ids if i in by_id]
    r_fits, overall, overall_n = _r_table(observations, vehicle_of)
    ocv_curve = _build_ocv_curve([p for o in observations for p in o.ocv_obs])

    scored = [by_id[i] for i in holdout_ids if i in by_id]
    holdout_maes = [
        mae for mae in (
            _holdout_mae(d, ocv_curve, _deployed_r(d.cell_count, r_fits, overall)) for d in scored
        ) if mae is not None
    ]
    if not holdout_maes:
        raise ValueError("No holdout logs could be evaluated; check holdout fraction.")
    maes = np.array(holdout_maes)
    median_mae = float(np.median(maes))
    real = (STATUS_OK, STATUS_UNUSABLE)  # loaded and not simulation, as before
    holdout_real = [i for i in holdout_ids if statuses[i] in real]
    return BatteryFitResult(
        real_log_count=sum(1 for s in statuses.values() if s in real),
        sim_log_count=sum(1 for s in statuses.values() if s == STATUS_SIM),
        skipped_log_count=sum(1 for s in statuses.values() if s not in (STATUS_OK, STATUS_SIM)),
        r_fits=r_fits,
        overall_r_median_mohm_per_cell=overall,
        overall_r_n=overall_n,
        ocv_curve=ocv_curve,
        holdout_log_count=len(holdout_real),
        holdout_scored_count=len(holdout_maes),
        holdout_median_mae_v_per_cell=median_mae,
        holdout_mean_mae_v_per_cell=float(np.mean(maes)),
        holdout_p75_mae_v_per_cell=float(np.percentile(maes, 75)),
        validation_target_met=(median_mae < VALIDATION_TARGET_V_PER_CELL),
        split_seed=seed,
        per_log_r=_per_log_r(observations, vehicle_of),
        fit_log_ids=[o.log_id for o in observations if o.r_obs or o.ocv_obs],
        holdout_log_ids=holdout_real,
        logs_without_manifest_row=logs_without_manifest_row,
    )


def _select_extracted(
    ids: Sequence[str], manifest_path: Path | None, max_per_vehicle: int
) -> tuple[list[str], dict[str, str], int]:
    """(kept log IDs, their vehicles, logs without a manifest row), as select_logs does for .ulg files."""
    check_max_per_vehicle(max_per_vehicle)
    if manifest_path is None:
        return sorted(ids), {}, 0
    manifest = read_manifest(manifest_path)
    listed = [i for i in ids if i in manifest]
    kept = cap_per_vehicle(listed, manifest, max_per_vehicle)
    return kept, {i: manifest[i] for i in kept}, len(ids) - len(listed)


def fit_obs_dir(
    obs_dirs: Path | Sequence[Path],
    manifest_path: Path | None = None,
    max_per_vehicle: int = DEFAULT_MAX_PER_VEHICLE,
    holdout_frac: float = DEFAULT_HOLDOUT_FRAC,
    seed: int = DEFAULT_SPLIT_SEED,
) -> BatteryFitResult:
    """fit_extracted over the files that extract_dir left in one or more folders.

    The folders (batches) merge as long as no log ID appears twice.  Logs
    without a manifest row are left out and counted once each: those marked
    <log_id>.no_manifest_row at extraction, plus extracted logs missing from
    this manifest.  At most max_per_vehicle logs of each vehicle (the smallest
    log IDs) are kept, so the result equals a one-pass run over the same logs.
    """
    dirs = [Path(obs_dirs)] if isinstance(obs_dirs, (str, Path)) else [Path(d) for d in obs_dirs]
    found = read_obs_dirs(dirs)
    marked = [i for i, (s, _) in found.items() if s == STATUS_NO_MANIFEST_ROW]
    statuses = {i: s for i, (s, _) in found.items() if s != STATUS_NO_MANIFEST_ROW}
    if not statuses:
        raise FileNotFoundError(f"No extracted battery files in {', '.join(map(str, dirs))}")
    kept, vehicle_of, without_row = _select_extracted(list(statuses), manifest_path, max_per_vehicle)
    datas = [load_named(found[i][1], i) for i in kept if statuses[i] == STATUS_OK]
    no_data = {i: statuses[i] for i in kept if statuses[i] != STATUS_OK}
    return fit_extracted(
        datas, vehicle_of, holdout_frac, seed,
        no_data=no_data, logs_without_manifest_row=without_row + len(marked),
    )


def analyze_logs(
    log_dir: str | Path,
    holdout_frac: float = DEFAULT_HOLDOUT_FRAC,
    seed: int = DEFAULT_SPLIT_SEED,
    manifest_path: Path | None = None,
    max_per_vehicle: int = DEFAULT_MAX_PER_VEHICLE,
    jobs: int = 1,
    loader: Callable[[Path], Any] = load_battery_topics,
) -> BatteryFitResult:
    """Extract the selected .ulg files of *log_dir* to a temporary folder, then fit.

    Simulation logs are detected via ``hardware.is_simulation()`` and
    excluded from all fits and statistics.

    Reproducibility: the pipeline is deterministic for fixed inputs.  Without
    a manifest the split permutes every .ulg file of log_dir, sorted, with the
    seed, as the one-pass analysis did (see ``split_logs``), so the numbers
    written to Rust, the Markdown report and the validation metric all come
    from ONE canonical run.
    """
    _check_holdout_frac(holdout_frac)
    with tempfile.TemporaryDirectory() as tmp:
        extract_dir(Path(log_dir), Path(tmp), manifest_path, max_per_vehicle, jobs, loader)
        return fit_obs_dir(Path(tmp), manifest_path, max_per_vehicle, holdout_frac, seed)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fit battery models (R_internal, OCV curve) from real PX4 flight logs.",
        epilog="Exit status: 0 extract done or target met, 1 target missed, 2 error.",
    )
    parser.add_argument(
        "--log-dir",
        default="data/downloaded",
        help="Directory containing *.ulg files (default: data/downloaded)",
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--extract-to", type=Path, default=None, metavar="DIR",
        help="extract the selected logs of --log-dir into DIR (one .npz per usable "
             "real log) and exit without fitting",
    )
    source.add_argument(
        "--obs-dir", type=Path, nargs="+", default=None, metavar="DIR",
        help="fit from the folders of one or more --extract-to runs instead of --log-dir",
    )
    add_selection_args(parser)
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
    return parser


def _run_extract(args: argparse.Namespace) -> int:
    print(f"Extracting battery data from {args.log_dir!r}...", file=sys.stderr, flush=True)
    counts = extract_dir(
        Path(args.log_dir), args.extract_to, args.manifest, args.max_per_vehicle, args.jobs
    )
    print(" | ".join(f"{k}: {v}" for k, v in counts.items()), file=sys.stderr)
    return 0


def _run_fit(args: argparse.Namespace) -> int:
    sources = args.obs_dir if args.obs_dir is not None else [args.log_dir]
    print(f"Fitting from {', '.join(map(str, sources))} (seed={args.seed})...",
          file=sys.stderr, flush=True)
    if args.obs_dir is not None:
        result = fit_obs_dir(
            args.obs_dir, args.manifest, args.max_per_vehicle, args.holdout_frac, args.seed
        )
    else:
        result = analyze_logs(
            args.log_dir, args.holdout_frac, args.seed,
            args.manifest, args.max_per_vehicle, args.jobs,
        )
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

    forbidden = forbidden_strings(*sources)
    write_json(Path(args.out_json), result_to_dict(result), forbidden)
    print(f"JSON results written to {args.out_json}", file=sys.stderr)
    write_markdown_summary(result, args.out_md, forbidden)
    print(f"Markdown summary written to {args.out_md}", file=sys.stderr)

    # Emit Rust constants for verbatim porting (same canonical run)
    if args.emit_rust:
        print(emit_rust_constants(result))

    return 0 if result.validation_target_met else 1


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.extract_to is not None:
            return _run_extract(args)
        return _run_fit(args)
    except (ManifestError, PrivateDataError, FileNotFoundError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
