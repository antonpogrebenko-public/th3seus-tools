"""Battery fit result types and the resistance thresholds the fit and its outputs share.

battery_fit computes these results and battery_output writes them; both import
from here, so neither imports the other's module at load time.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

# ---------------------------------------------------------------------------
# Resistance-table thresholds (used by the fit and quoted in the Markdown)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


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

    Counts are over the selected logs: ``real_log_count`` is every log that
    loaded and is not simulation (as before); ``skipped_log_count`` is every
    non-simulation log without usable data, load errors included.  ``fit_log_ids`` are the
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
