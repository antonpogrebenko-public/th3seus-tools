"""Outputs of the battery fit: JSON dict, Markdown summary, Rust constants."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from .battery_model import (
    MIN_R_OBSERVATIONS_PER_CLASS,
    R_PLAUSIBLE_FLOOR_MOHM_PER_CELL,
    BatteryFitResult,
)
from .outputs import write_text


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
