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
    python -m log_validation.battery_fit --obs-dir OBS --manifest M.csv --out-json ... --out-md ...
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from itertools import groupby, islice
from pathlib import Path
from typing import Any, Optional

import numpy as np
from pyulog import ULog

from .hardware import extract_hardware_info, is_simulation
from .manifest import (
    DEFAULT_MAX_PER_VEHICLE,
    ManifestError,
    add_selection_args,
    read_manifest,
    select_logs,
)
from .outputs import PrivateDataError, forbidden_strings, write_json, write_text
from .parallel import run_per_log
from .ulog_loader import US_TO_S

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
# Extract and fit step constants (the values of the one-pass analysis, named)
# ---------------------------------------------------------------------------

# The only topic the extract step parses.  The log header (msg_info_dict and
# initial_parameters, which extract_hardware_info needs) is read regardless.
BATTERY_TOPIC: str = "battery_status"

# Pack checks: a usable log has 1 to MAX_CELL_COUNT cells, a voltage that moves
# by at least MIN_VOLTAGE_RANGE_V and a peak current of at least MIN_PEAK_CURRENT_A.
MAX_CELL_COUNT: int = 14
MIN_VOLTAGE_RANGE_V: float = 0.1
MIN_PEAK_CURRENT_A: float = 2.0

# battery_status.remaining serves as SoC only when it moves: by more than this
# for the OCV fit, by at least this for holdout scoring (as before).
SOC_MIN_RANGE: float = 0.02

# Depth of discharge assumed at the end of a log when SoC comes from
# discharged_mah: the capacity estimate is max(discharged_mah) / this.
ASSUMED_END_DEPTH_OF_DISCHARGE: float = 0.8

# Held-out median MAE below which the fit is reported as a PASS (V/cell).
VALIDATION_TARGET_V_PER_CELL: float = 0.1

# Status of each selected log after the extract step.  An "ok" log is saved as
# <log_id>.npz; any other status is saved as an empty <log_id>.<status> file,
# so the fit step still places that log in the split (see fit_extracted).
STATUS_OK = "ok"
STATUS_SIM = "sim"
STATUS_UNUSABLE = "unusable"
STATUS_WORKER_DIED = "worker_died"
STATUSES = (STATUS_OK, STATUS_SIM, STATUS_UNUSABLE, STATUS_WORKER_DIED)
NPZ_SUFFIX = ".npz"
_SUFFIX_OF_STATUS = {STATUS_OK: NPZ_SUFFIX, **{s: f".{s}" for s in STATUSES[1:]}}
_STATUS_OF_SUFFIX = {suffix: status for status, suffix in _SUFFIX_OF_STATUS.items()}
_REQUIRED_KEYS = ("log_id", "cell_count", "t_s", "v", "i")
_OPTIONAL_KEYS = ("remaining", "discharged_mah")


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, eq=False)
class LogBatteryData:
    """battery_status series of one usable real log, as the fit step reads it.

    eq=False: a field-wise == over numpy arrays is ambiguous; compare the
    arrays with numpy instead.
    """

    log_id: str
    cell_count: int
    t_s: np.ndarray
    v: np.ndarray
    i: np.ndarray
    remaining: np.ndarray | None
    discharged_mah: np.ndarray | None


@dataclass(frozen=True)
class PerCellRFit:
    """Fitted internal resistance for one cell-count class.

    ``deployed_mohm_per_cell`` is the value actually used downstream (report,
    validation, Rust): it equals ``median_mohm_per_cell`` when the class has
    enough observations, otherwise it falls back to the pooled median.
    ``low_n`` flags when the fallback was applied.  The percentiles are over
    the class's step events; ``n_vehicles`` counts the distinct vehicles of
    the logs behind them (a log without a vehicle counts as its own).
    """

    cell_count: int
    median_mohm_per_cell: float
    p25_mohm_per_cell: float
    p75_mohm_per_cell: float
    n_observations: int
    deployed_mohm_per_cell: float
    low_n: bool
    p10_mohm_per_cell: float
    p90_mohm_per_cell: float
    n_vehicles: int


@dataclass(frozen=True)
class OcvCurve:
    """Fitted OCV curve: (soc, v_per_cell) table, monotone increasing."""

    soc_knots: list[float]
    v_per_cell_knots: list[float]
    n_total_observations: int

    def interpolate(self, soc: float) -> float:
        """Return per-cell OCV at a given state of charge [0, 1]."""
        return float(np.interp(soc, self.soc_knots, self.v_per_cell_knots))


@dataclass(frozen=True)
class BatteryFitResult:
    """Aggregate output of the battery fitting pipeline.

    Counts are over the selected logs: ``real_log_count`` is every selected
    log not detected as simulation, including those without usable battery
    data, which ``skipped_log_count`` counts.  ``fit_log_ids`` are the
    fit-side logs that gave at least one resistance or OCV observation;
    ``holdout_log_ids`` are the real logs of the holdout side; ``per_log_r``
    has one row per fit-side log with resistance observations.
    """

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
    per_log_r: list[dict]  # log_id, vehicle_id, cell_count, median_mohm_per_cell
    fit_log_ids: list[str]
    holdout_log_ids: list[str]
    logs_without_manifest_row: int


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
# Extract step: one log -> LogBatteryData -> <log_id>.npz
# ---------------------------------------------------------------------------


def load_battery_topics(path: Path) -> ULog:
    """Parse only battery_status; the log header extract_hardware_info needs is always read."""
    return ULog(str(path), message_name_filter_list=[BATTERY_TOPIC])


def extract_log(
    path: Path, loader: Callable[[Path], Any] = load_battery_topics
) -> tuple[str, LogBatteryData | None]:
    """Read one log: ("ok", data), ("sim", None) or ("unusable", None).

    Unusable: the log fails to load, has no battery_status, or its pack fails
    the checks of the one-pass analysis (cell count, voltage range, current).
    """
    try:
        ulog = loader(path)
    except Exception:
        return STATUS_UNUSABLE, None
    if _is_simulation_log(ulog):
        return STATUS_SIM, None
    data = _battery_data(Path(path).stem, ulog)
    return (STATUS_UNUSABLE, None) if data is None else (STATUS_OK, data)


def _is_simulation_log(ulog: Any) -> bool:
    """SITL/HIL check; a log whose header cannot be read counts as real, as before."""
    try:
        hw = extract_hardware_info(ulog.msg_info_dict, ulog.initial_parameters)
    except Exception:
        return False
    return is_simulation(hw)


def _battery_data(log_id: str, ulog: Any) -> LogBatteryData | None:
    if BATTERY_TOPIC not in {d.name for d in ulog.data_list}:
        return None
    try:
        ds = ulog.get_dataset(BATTERY_TOPIC, multi_instance=0)
        v = np.asarray(ds.data["voltage_v"], dtype=float)
        i = np.asarray(ds.data["current_a"], dtype=float)
        t_s = np.asarray(ds.data["timestamp"], dtype=float) * US_TO_S
        cells = int(ds.data["cell_count"][0])
    except (KeyError, IndexError, ValueError):
        return None
    if not _pack_is_usable(v, i, cells):
        return None
    return LogBatteryData(
        log_id=log_id,
        cell_count=cells,
        t_s=t_s,
        v=v,
        i=i,
        remaining=_optional_series(ds.data, "remaining"),
        discharged_mah=_optional_series(ds.data, "discharged_mah"),
    )


def _pack_is_usable(v: np.ndarray, i: np.ndarray, cells: int) -> bool:
    """The one-pass analysis's checks, kept as rejections so a NaN passes as it did."""
    if cells <= 0 or cells > MAX_CELL_COUNT:
        return False
    if v.max() - v.min() < MIN_VOLTAGE_RANGE_V:
        return False
    if i.max() < MIN_PEAK_CURRENT_A:
        return False
    return True


def _optional_series(data: Mapping[str, Any], name: str) -> np.ndarray | None:
    return np.asarray(data[name], dtype=float) if name in data else None


def save_extracted(data: LogBatteryData, out_dir: Path) -> Path:
    """Write <out_dir>/<log_id>.npz; an interrupted write never takes that name."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{data.log_id}{NPZ_SUFFIX}"
    partial = out / f"{data.log_id}{NPZ_SUFFIX}.part"
    arrays = {
        "log_id": np.array(data.log_id),
        "cell_count": np.array(data.cell_count),
        "t_s": data.t_s,
        "v": data.v,
        "i": data.i,
    }
    optional = {k: getattr(data, k) for k in _OPTIONAL_KEYS if getattr(data, k) is not None}
    with open(partial, "wb") as fh:
        np.savez_compressed(fh, **arrays, **optional)
    os.replace(partial, path)
    return path


def load_extracted(path: Path) -> LogBatteryData:
    """Read a file written by save_extracted; ValueError if it is malformed."""
    name = Path(path).name
    with np.load(path, allow_pickle=False) as npz:
        missing = [k for k in _REQUIRED_KEYS if k not in npz.files]
        if missing:
            raise ValueError(f"{name}: missing {', '.join(missing)}")
        arrays = {k: npz[k] for k in npz.files}
    data = LogBatteryData(
        log_id=str(arrays["log_id"]),
        cell_count=int(arrays["cell_count"]),
        t_s=arrays["t_s"],
        v=arrays["v"],
        i=arrays["i"],
        remaining=arrays.get("remaining"),
        discharged_mah=arrays.get("discharged_mah"),
    )
    series = [data.t_s, data.v, data.i, data.remaining, data.discharged_mah]
    shapes = {a.shape for a in series if a is not None}
    if len(shapes) != 1 or len(next(iter(shapes))) != 1:
        raise ValueError(f"{name}: series must be one-dimensional and of one length")
    return data


def extract_dir(
    log_dir: Path,
    out_dir: Path,
    manifest_path: Path | None = None,
    max_per_vehicle: int = DEFAULT_MAX_PER_VEHICLE,
    jobs: int = 1,
    loader: Callable[[Path], Any] = load_battery_topics,
) -> dict[str, int]:
    """Extract the selected logs of log_dir into out_dir; return the count per status.

    The selection is manifest.select_logs (manifest rows, per-vehicle cap).
    Each selected log leaves exactly one file in out_dir: <log_id>.npz when
    usable, else an empty <log_id>.<status>.  With jobs > 1 the logs run in
    worker processes (parallel.run_per_log); a log whose worker dies gets the
    status "worker_died" and the other logs are still extracted; loader must
    then be a top-level function so it pickles.  The counts also hold the
    selection's logs_without_manifest_row and capped_out.
    """
    selection = select_logs(Path(log_dir), manifest_path, max_per_vehicle)
    if not selection.paths:
        raise FileNotFoundError(f"No .ulg files selected in {str(log_dir)!r}")
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    tasks = [(p, out, loader) for p in selection.paths]
    statuses = run_per_log(_extract_task, tasks, jobs, _extract_died)
    for path, status in zip(selection.paths, statuses):
        _record_status(out, path.stem, status)
    return {
        **{s: statuses.count(s) for s in STATUSES},
        "logs_without_manifest_row": selection.logs_without_manifest_row,
        "capped_out": selection.capped_out,
    }


def _extract_task(task: tuple[Path, Path, Callable[[Path], Any]]) -> str:
    """Worker: extract one log, save it when usable, return its status."""
    path, out_dir, loader = task
    status, data = extract_log(path, loader)
    if data is not None:
        save_extracted(data, out_dir)
    return status


def _extract_died(task: tuple[Path, Path, Callable[[Path], Any]]) -> str:
    return STATUS_WORKER_DIED


def _record_status(out_dir: Path, log_id: str, status: str) -> None:
    """Leave one file for log_id: drop the files of its other statuses, mark a non-ok one."""
    for other, suffix in _SUFFIX_OF_STATUS.items():
        if other != status:
            (out_dir / f"{log_id}{suffix}").unlink(missing_ok=True)
    if status != STATUS_OK:
        (out_dir / f"{log_id}{_SUFFIX_OF_STATUS[status]}").touch()


def _read_statuses(obs_dir: Path) -> dict[str, str]:
    """{log_id: status} from the files extract_dir left; other files are ignored."""
    found = sorted(
        (p.stem, _STATUS_OF_SUFFIX[p.suffix])
        for p in Path(obs_dir).iterdir()
        if p.is_file() and p.suffix in _STATUS_OF_SUFFIX
    )
    repeated = sorted(i for i, n in Counter(i for i, _ in found).items() if n > 1)
    if repeated:
        raise ValueError(f"log ID with more than one extracted file: {', '.join(repeated[:5])}")
    return dict(found)


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
    log to its extract status ("sim", "unusable" or "worker_died"): those logs
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
    n_sim = sum(1 for s in statuses.values() if s == STATUS_SIM)
    holdout_real = [i for i in holdout_ids if statuses[i] != STATUS_SIM]
    return BatteryFitResult(
        real_log_count=len(statuses) - n_sim,
        sim_log_count=n_sim,
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
    if manifest_path is None:
        return sorted(ids), {}, 0
    manifest = read_manifest(manifest_path)
    listed = [i for i in ids if i in manifest]
    by_vehicle = sorted(listed, key=lambda i: (manifest[i], i))
    # Sorted by vehicle, then log ID: the smallest IDs of each vehicle win.
    kept = sorted(
        i for _, group in groupby(by_vehicle, key=lambda i: manifest[i])
        for i in islice(group, max_per_vehicle)
    )
    return kept, {i: manifest[i] for i in kept}, len(ids) - len(listed)


def _load_named(obs_dir: Path, log_id: str) -> LogBatteryData:
    data = load_extracted(obs_dir / f"{log_id}{NPZ_SUFFIX}")
    if data.log_id != log_id:
        raise ValueError(f"{log_id}{NPZ_SUFFIX} holds log {data.log_id}")
    return data


def fit_obs_dir(
    obs_dir: Path,
    manifest_path: Path | None = None,
    max_per_vehicle: int = DEFAULT_MAX_PER_VEHICLE,
    holdout_frac: float = DEFAULT_HOLDOUT_FRAC,
    seed: int = DEFAULT_SPLIT_SEED,
) -> BatteryFitResult:
    """fit_extracted over the files that extract_dir left in obs_dir.

    obs_dir may merge several extract_dir runs (batches) as long as no log ID
    appears twice.  With a manifest, logs without a row are left out and
    counted, and at most max_per_vehicle logs of each vehicle (the smallest log
    IDs) are kept, so the result equals a one-pass run over the same logs.
    """
    statuses = _read_statuses(Path(obs_dir))
    if not statuses:
        raise FileNotFoundError(f"No extracted battery files in {str(obs_dir)!r}")
    kept, vehicle_of, without_row = _select_extracted(list(statuses), manifest_path, max_per_vehicle)
    datas = [_load_named(Path(obs_dir), i) for i in kept if statuses[i] == STATUS_OK]
    no_data = {i: statuses[i] for i in kept if statuses[i] != STATUS_OK}
    return fit_extracted(
        datas, vehicle_of, holdout_frac, seed,
        no_data=no_data, logs_without_manifest_row=without_row,
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
        counts = extract_dir(Path(log_dir), Path(tmp), manifest_path, max_per_vehicle, jobs, loader)
        result = fit_obs_dir(Path(tmp), manifest_path, max_per_vehicle, holdout_frac, seed)
    # The temporary folder holds only the selected logs; logs of log_dir
    # without a manifest row were counted by the selection.
    return replace(result, logs_without_manifest_row=counts["logs_without_manifest_row"])


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def result_to_dict(r: BatteryFitResult) -> dict:
    """Serialise BatteryFitResult to a JSON-compatible dict."""
    return {
        "real_log_count": r.real_log_count,
        "sim_log_count": r.sim_log_count,
        "skipped_log_count": r.skipped_log_count,
        "logs_without_manifest_row": r.logs_without_manifest_row,
        "split_seed": r.split_seed,
        "r_fits": [
            {
                "cell_count": f.cell_count,
                "median_mohm_per_cell": f.median_mohm_per_cell,
                "p10_mohm_per_cell": f.p10_mohm_per_cell,
                "p25_mohm_per_cell": f.p25_mohm_per_cell,
                "p75_mohm_per_cell": f.p75_mohm_per_cell,
                "p90_mohm_per_cell": f.p90_mohm_per_cell,
                "n_observations": f.n_observations,
                "n_vehicles": f.n_vehicles,
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
        "per_log_r": [dict(row) for row in r.per_log_r],
        "fit_log_ids": list(r.fit_log_ids),
        "holdout_log_ids": list(r.holdout_log_ids),
    }


def _per_log_lines(r: BatteryFitResult) -> list[str]:
    """Markdown sections listing the logs behind the fit and the holdout."""
    lines = [
        "## Per-Log Resistance (fit side)",
        "",
        "| Log ID | Vehicle | Cell count | Median (mΩ/cell) |",
        "|---|---|---|---|",
    ]
    lines += [
        f"| {row['log_id']} | {row['vehicle_id'] or '-'} | {row['cell_count']}S | "
        f"{row['median_mohm_per_cell']:.2f} |"
        for row in r.per_log_r
    ]
    return lines + [
        "",
        "## Log IDs",
        "",
        f"Fit, with at least one observation ({len(r.fit_log_ids)}): "
        + ", ".join(r.fit_log_ids),
        "",
        f"Holdout, real logs ({len(r.holdout_log_ids)}): " + ", ".join(r.holdout_log_ids),
        "",
    ]


def write_markdown_summary(
    r: BatteryFitResult, path: str | Path, forbidden: Sequence[str] = ()
) -> None:
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
        f"| Real logs (fit and holdout) | {r.real_log_count} |",
        f"| SIM logs excluded | {r.sim_log_count} |",
        f"| Skipped (load error or no usable battery data) | {r.skipped_log_count} |",
        f"| Holdout (validation only) | {r.holdout_log_count} |",
        f"| Fit logs with at least one observation | {len(r.fit_log_ids)} |",
        f"| Logs without a manifest row (left out) | {r.logs_without_manifest_row} |",
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
        "| Cell count | Median (mΩ/cell) | p10 | p90 | Deployed (mΩ/cell) | IQR "
        "| n step events | Vehicles | fallback reason |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for f in r.r_fits:
        iqr = f"{f.p25_mohm_per_cell:.2f}–{f.p75_mohm_per_cell:.2f}"
        if not f.low_n:
            reason = "none"
        elif f.n_observations < MIN_R_OBSERVATIONS_PER_CLASS:
            reason = f"low-n (<{MIN_R_OBSERVATIONS_PER_CLASS}) → pooled"
        else:
            reason = (
                f"implausible (<{R_PLAUSIBLE_FLOOR_MOHM_PER_CELL:.0f} mΩ/cell) → pooled"
            )
        lines.append(
            f"| {f.cell_count}S | {f.median_mohm_per_cell:.2f} | "
            f"{f.p10_mohm_per_cell:.2f} | {f.p90_mohm_per_cell:.2f} | "
            f"{f.deployed_mohm_per_cell:.2f} | {iqr} | {f.n_observations} | "
            f"{f.n_vehicles} | {reason} |"
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
    lines += _per_log_lines(r)

    write_text(Path(path), "\n".join(lines) + "\n", forbidden)


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
        "--obs-dir", type=Path, default=None, metavar="DIR",
        help="fit from the files of one or more --extract-to runs instead of --log-dir",
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
    source = args.obs_dir if args.obs_dir is not None else args.log_dir
    print(f"Fitting from {str(source)!r} (seed={args.seed})...", file=sys.stderr, flush=True)
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

    forbidden = forbidden_strings(source)
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
