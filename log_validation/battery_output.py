"""Outputs of the battery fit: JSON dict, Markdown summary, Rust constants."""

from __future__ import annotations

import numbers
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .battery_capacity import MIN_CLASS_VEHICLES
from .battery_model import (
    CELL_V_MAX,
    CELL_V_MIN,
    MIN_R_OBSERVATIONS_PER_CLASS,
    BatteryFitResult,
    PerCellRFit,
)
from .outputs import write_text


def fallback_reason(f: PerCellRFit) -> str | None:
    """Why a class deploys the pooled median, or None when it deploys its own fit."""
    if not f.low_n:
        return None
    return f"low-n (fewer than {MIN_R_OBSERVATIONS_PER_CLASS} step events)"


def _pooled_vehicles_lines(r: BatteryFitResult) -> list[str]:
    if r.pooled_r_over_vehicles_mohm_per_cell is None:
        return []
    return [
        f"Pooled median over vehicles: {r.pooled_r_over_vehicles_mohm_per_cell:.2f} mohm/cell "
        f"({r.pooled_r_vehicles} vehicles); a cell count with fewer than "
        f"{MIN_R_OBSERVATIONS_PER_CLASS} step events, and any cell count without a row, "
        "deploys it (the **Deployed** column is what Rust uses).",
        "",
    ]


def result_to_dict(r: BatteryFitResult) -> dict:
    """Serialise BatteryFitResult to a JSON-compatible dict."""
    return {
        "real_log_count": r.real_log_count,
        "sim_log_count": r.sim_log_count,
        "skipped_log_count": r.skipped_log_count,
        "cell_count_mismatch_count": r.cell_count_mismatch_count,
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
                "fallback_reason": fallback_reason(f),
            }
            for f in r.r_fits
        ],
        "capacity_bounds_mah": list(r.capacity_bounds_mah),
        "r_fits_by_capacity": [asdict(f) for f in r.r_fits_by_capacity],
        "overall_r_median_mohm_per_cell": r.overall_r_median_mohm_per_cell,
        "overall_r_n": r.overall_r_n,
        "pooled_r_over_vehicles_mohm_per_cell": r.pooled_r_over_vehicles_mohm_per_cell,
        "pooled_r_vehicles": r.pooled_r_vehicles,
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
        "holdout_scored_log_ids": list(r.holdout_scored_log_ids),
        "fit_logs_without_samples": r.fit_logs_without_samples,
        "holdout_logs_not_scored": r.holdout_logs_not_scored,
        "holdout_by_cell_count": [asdict(c) for c in r.holdout_by_cell_count],
        "step_check": {
            "n_steps": r.step_check.n_steps,
            "n_vehicles": r.step_check.n_vehicles,
            "median_err_v_per_cell": r.step_check.median_err_v_per_cell,
            "p75_err_v_per_cell": r.step_check.p75_err_v_per_cell,
        },
    }


def _per_log_lines(r: BatteryFitResult) -> list[str]:
    """Markdown sections listing the logs behind the fit and the holdout."""
    lines = [
        "## Per-Log Resistance (fit side)",
        "",
        "| Log ID | Vehicle | Cell count | Median (mohm/cell) |",
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
        f"Holdout, scored by the MAE or the step check ({len(r.holdout_scored_log_ids)}): "
        + ", ".join(r.holdout_scored_log_ids),
        "",
    ]


def _fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.4f}"


def _capacity_lines(r: BatteryFitResult) -> list[str]:
    """The per cell count and pack-capacity table (the Rust constants do not use it)."""
    bounds = ", ".join(f"{b:g}" for b in r.capacity_bounds_mah)
    lines = [
        "",
        "## Internal Resistance by Pack Capacity",
        "",
        f"Pack capacity is the log's battery capacity parameter; class bounds (mAh): {bounds}.  "
        f"A class with fewer than {MIN_CLASS_VEHICLES} vehicles is left out.  "
        "Spread only: the Rust constants stay per cell count.",
        "",
    ]
    if not r.r_fits_by_capacity:
        return lines + ["No cell count and capacity class has enough vehicles."]
    lines += [
        "| Cell count | Capacity (mAh) | Median (mohm/cell) | p10 | p90 | n step events | Vehicles |",
        "|---|---|---|---|---|---|---|",
    ]
    for f in r.r_fits_by_capacity:
        lines.append(
            f"| {f.cell_count}S | {f.capacity_class} | {f.median_mohm_per_cell:.2f} | "
            f"{f.p10_mohm_per_cell:.2f} | {f.p90_mohm_per_cell:.2f} | {f.n_steps} | "
            f"{f.n_vehicles} |"
        )
    return lines


def _cell_count_check_lines(r: BatteryFitResult) -> list[str]:
    """The held-out check per cell count, with a sentence on the sign of the error."""
    lines = [
        "## Held-out check per cell count",
        "",
        "Median over the scored held-out logs of each cell count, with the Deployed R.  "
        "The signed error is predicted minus measured voltage per cell; positive means "
        "the model predicts less sag than the packs show, or that the open-circuit curve "
        "sits above these packs.",
        "",
        "| Cells | Scored logs | Deployed mohm | Median error (V/cell) | Median signed error (V/cell) |",
        "|---|---|---|---|---|",
    ]
    lines += [
        f"| {c.cell_count}S | {c.scored_logs} | {c.deployed_mohm_per_cell:.2f} | "
        f"{c.median_mae_v_per_cell:.4f} | {c.median_signed_err_v_per_cell:.4f} |"
        for c in r.holdout_by_cell_count
    ]
    return lines + [""]


def write_markdown_summary(
    r: BatteryFitResult, path: str | Path, forbidden: Sequence[str] = ()
) -> None:
    """Write a short Markdown summary of the fit results."""
    sc = r.step_check
    lines = [
        "# Battery Model Fit",
        "",
        "Fit from real PX4 flight logs (SIM excluded via `hardware.is_simulation`).",
        "",
        f"Canonical run: deterministic, split seed = `{r.split_seed}`.  "
        "The R table, OCV table, and held-out MAE below all derive from this "
        "single run; `--emit-rust` prints the tables in the format "
        "`hitl-physics/src/battery.rs` uses.",
        "",
        "## Corpus",
        "",
        "| | Count |",
        "|---|---|",
        f"| Real logs (fit and holdout) | {r.real_log_count} |",
        f"| SIM logs excluded | {r.sim_log_count} |",
        f"| Skipped (load error or no usable battery data) | {r.skipped_log_count} |",
        "| Left out: cell count does not match the pack voltage "
        f"(median outside {CELL_V_MIN:.1f} to {CELL_V_MAX:.1f} V per cell) | "
        f"{r.cell_count_mismatch_count} |",
        f"| Holdout (validation only) | {r.holdout_log_count} |",
        f"| Fit logs with at least one observation | {len(r.fit_log_ids)} |",
        f"| Usable fit logs without a step or OCV sample | {r.fit_logs_without_samples} |",
        f"| Usable holdout logs scored by neither check | {r.holdout_logs_not_scored} |",
        f"| Logs without a manifest row (left out) | {r.logs_without_manifest_row} |",
        "",
        "## Internal Resistance",
        "",
        f"Pooled median over step events: {r.overall_r_median_mohm_per_cell:.2f} mohm/cell "
        f"(n={r.overall_r_n} steps).",
        "",
        *_pooled_vehicles_lines(r),
        "| Cell count | Median (mohm/cell) | p10 | p90 | Deployed (mohm/cell) | IQR "
        "| n step events | Vehicles | fallback reason |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for f in r.r_fits:
        iqr = f"{f.p25_mohm_per_cell:.2f} to {f.p75_mohm_per_cell:.2f}"
        why = fallback_reason(f)
        reason = "none" if why is None else f"{why} -> pooled over vehicles"
        lines.append(
            f"| {f.cell_count}S | {f.median_mohm_per_cell:.2f} | "
            f"{f.p10_mohm_per_cell:.2f} | {f.p90_mohm_per_cell:.2f} | "
            f"{f.deployed_mohm_per_cell:.2f} | {iqr} | {f.n_observations} | "
            f"{f.n_vehicles} | {reason} |"
        )

    lines += _capacity_lines(r)

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
        f"| Held-out logs with a voltage error (MAE) | {r.holdout_scored_count} of "
        f"{r.holdout_log_count} real held-out logs, usable or not |",
        f"| Median MAE (V/cell) | **{r.holdout_median_mae_v_per_cell:.4f}** |",
        f"| Mean MAE (V/cell) | {r.holdout_mean_mae_v_per_cell:.4f} |",
        f"| p75 MAE (V/cell) | {r.holdout_p75_mae_v_per_cell:.4f} |",
        f"| Step check: steps (vehicles) | {sc.n_steps} ({sc.n_vehicles}) |",
        f"| Step check: median error (V/cell) | {_fmt(sc.median_err_v_per_cell)} |",
        f"| Step check: p75 error (V/cell) | {_fmt(sc.p75_err_v_per_cell)} |",
        f"| Target < 0.1 V/cell | {'PASS' if r.validation_target_met else 'FAIL'} |",
        "",
        *_cell_count_check_lines(r),
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
        "step events is too noisy to trust and falls back to the median pooled over vehicles; "
        "a measured low resistance is deployed as it is.  "
        "The **Deployed** column reflects this and matches Rust.",
        "",
    ]
    lines += _per_log_lines(r)

    write_text(Path(path), "\n".join(lines) + "\n", forbidden)


def emit_rust_constants(r: BatteryFitResult) -> str:
    """The Rust constants of a result (see ``rust_constants_text``)."""
    return rust_constants_text(result_to_dict(r))


def _whole(record: Mapping[str, Any], key: str) -> int:
    """`record[key]` as an int for the Rust text; ValueError unless it is a whole number (a
    string or a fraction would otherwise be written into the Rust code or its comments)."""
    value = record[key]
    if (isinstance(value, bool) or not isinstance(value, numbers.Real)
            or not float(value).is_integer()):
        raise ValueError(f"battery fit: {key} is not a whole number")
    return int(value)


def _r_note(battery: Mapping[str, Any], f: Mapping[str, Any], n: int) -> str:
    """The comment of one R row; a file from before the vehicle-pooled median names its stored reason."""
    if not f["low_n"]:
        return f"n={n}"
    if "pooled_r_over_vehicles_mohm_per_cell" in battery:
        return f"n={n}, fewer than {MIN_R_OBSERVATIONS_PER_CLASS} step events -> pooled over vehicles"
    reason = f.get("fallback_reason")
    return f"n={n}, {reason} -> pooled median" if reason else f"n={n} -> pooled median"


def _r_table_lines(battery: Mapping[str, Any]) -> list[str]:
    lines = [
        "// FITTED_R_TABLE: deployed per-cell R (mohm/cell), keyed by cell count.",
        f"// Canonical run, split seed={_whole(battery, 'split_seed')}, "
        f"{_whole(battery, 'real_log_count')} real logs, "
        f"{_whole(battery, 'sim_log_count')} SIM excluded.",
        "const FITTED_R_TABLE: &[(u8, f64)] = &[",
    ]
    for f in battery["r_fits"]:
        n = _whole(f, "n_observations")
        lines.append(
            f"    ({_whole(f, 'cell_count')}, {f['deployed_mohm_per_cell']:.4f}),  // {_r_note(battery, f, n)}"
        )
    return lines + ["];"]


def _default_line(battery: Mapping[str, Any]) -> str:
    """The default-R declaration: the vehicle-pooled median, or, for a file from before
    it was written, the step-pooled one."""
    head = "const FITTED_R_DEFAULT_MOHM_PER_CELL: f64 = "
    pooled = battery.get("pooled_r_over_vehicles_mohm_per_cell")
    if pooled is None:
        return (f"{head}{battery['overall_r_median_mohm_per_cell']:.4f};  "
                f"// pooled over step events, n={_whole(battery, 'overall_r_n')}")
    return f"{head}{pooled:.4f};  // pooled over {_whole(battery, 'pooled_r_vehicles')} vehicles"


def rust_constants_text(battery: Mapping[str, Any]) -> str:
    """Render the deployed R table + OCV table as Rust source constants.

    ``battery`` is the result's JSON dict.  The output is in the format
    ``hitl-physics/src/battery.rs`` uses, so the tables can be copied there as
    they are; it holds nothing from the pack-capacity rows.  The counts must be
    whole numbers (ValueError otherwise).
    """
    lines = _r_table_lines(battery)
    lines.append("")
    lines.append(_default_line(battery))
    lines.append("")
    lines.append("const OCV_TABLE: &[(f64, f64)] = &[")
    ocv = battery["ocv_curve"]
    for soc, v in zip(ocv["soc_knots"], ocv["v_per_cell_knots"]):
        lines.append(f"    ({soc:.4f}, {v:.4f}),")
    lines.append("];")
    return "\n".join(lines)
