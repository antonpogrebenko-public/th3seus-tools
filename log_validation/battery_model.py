"""Battery fit result types and the resistance thresholds the fit and its outputs share.

battery_fit computes these results and battery_output writes them; both import
from here, so neither imports the other's module at load time.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .battery_capacity import DEFAULT_CAPACITY_BOUNDS_MAH, CapacityClassRFit

# ---------------------------------------------------------------------------
# Resistance-table thresholds (used by the fit and quoted in the Markdown)
# ---------------------------------------------------------------------------

# Plausible per-cell voltage range (V) of a pack's median voltage; a log whose
# median pack voltage divided by its cell count falls outside it has a wrong cell count.
CELL_COUNT_CHECK_V_MIN: float = 3.0
CELL_COUNT_CHECK_V_MAX: float = 4.4

# Minimum number of step-event observations for a per-cell-count R median to be
# trusted.  Cell counts below this fall back to the pooled median because their
# sample is too small to be stable across runs.
MIN_R_OBSERVATIONS_PER_CLASS: int = 200


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PerCellRFit:
    """Fitted internal resistance for one cell-count class.

    ``deployed_mohm_per_cell`` is the value actually used downstream (report,
    validation, Rust): it equals ``median_mohm_per_cell`` when the class has
    at least ``MIN_R_OBSERVATIONS_PER_CLASS`` step events, otherwise it falls
    back to the median pooled over vehicles.  ``low_n`` flags when the
    fallback was applied (a measured low resistance is deployed as it is).  The percentiles are over
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
class StepCheck:
    """Held-out check of R on current steps alone; it never reads PX4's ``remaining``.

    Errors are |measured - predicted| voltage change per cell over every step
    of the scored holdout logs, pooled.  Both percentiles are None without
    steps.  ``n_vehicles`` counts the vehicles of logs that gave a step (a log
    without a vehicle counts as its own).
    """

    n_steps: int
    n_vehicles: int
    median_err_v_per_cell: float | None
    p75_err_v_per_cell: float | None


@dataclass(frozen=True)
class CellCountCheck:
    """Held-out voltage check of the deployed R for one cell count.

    Medians over the scored holdout logs of that cell count.  The signed error is
    (predicted - measured) per cell: positive when the model predicts a higher
    voltage than measured, that is, it under-predicts the sag.
    """

    cell_count: int
    scored_logs: int
    deployed_mohm_per_cell: float
    median_mae_v_per_cell: float
    median_signed_err_v_per_cell: float


@dataclass(frozen=True)
class BatteryFitResult:
    """Aggregate output of the battery fitting pipeline.

    Counts are over the selected logs: ``real_log_count`` is every log that
    loaded and is not simulation (as before); ``skipped_log_count`` is every
    non-simulation log without usable data, load errors included.  ``fit_log_ids`` are the
    fit-side logs that gave at least one resistance or OCV observation;
    ``holdout_log_ids`` are the real logs of the holdout side; ``per_log_r``
    has one row per fit-side log with resistance observations.
    ``fit_logs_without_samples`` counts the usable fit-side logs that gave no
    observation, and ``holdout_logs_not_scored`` the usable holdout-side logs
    in neither score (not in ``holdout_scored_log_ids``), so every selected log
    is in exactly one of: fit_log_ids, holdout_scored_log_ids, those two
    counts, sim_log_count, skipped_log_count and cell_count_mismatch_count.
    ``cell_count_mismatch_count`` counts the real logs left out because the
    logged cell count does not match the pack voltage; they count in
    ``real_log_count`` and, when held out, in ``holdout_log_ids``.
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
    step_check: StepCheck
    holdout_scored_log_ids: tuple[str, ...]  # sorted; scored by the MAE or the step check
    fit_logs_without_samples: int  # usable fit-side logs with no step and no OCV sample
    holdout_logs_not_scored: int  # usable holdout-side logs scored by neither check
    r_fits_by_capacity: tuple[CapacityClassRFit, ...] = ()  # per cell count and capacity class
    capacity_bounds_mah: tuple[float, ...] = DEFAULT_CAPACITY_BOUNDS_MAH
    cell_count_mismatch_count: int = 0
    pooled_r_over_vehicles_mohm_per_cell: float | None = None
    pooled_r_vehicles: int = 0
    holdout_by_cell_count: tuple[CellCountCheck, ...] = ()
