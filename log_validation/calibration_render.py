"""The calibration report in markdown: eight sections, each a small function."""

from __future__ import annotations

import json
import math
from typing import Any, Mapping, Sequence

from .calibration_counts import PassCounts, battery_counts, entry_counts
from .calibration_noise import TICK_HZ, noise_before_after, vibration_evidence
from .calibration_ranges import ALL, RangeRow, log_id_lists

SECTIONS = ("Run", "IMU noise before and after", "Battery", "Vehicle response",
            "Reference airframes", "Randomization ranges",
            "Gaps TH3SEUS does not model", "Counts")
RUN_DATE_PREFIX = "- Run date: "
NA = "n/a"
RESPONSE_QUANTITIES = ("effectiveness.", "lag_minus_filter_s.", "hover.command", "motor_lag_s",
                       "rate.", "inflight_gyro_noise.")


def _num(value: Any, spec: str = ".4g") -> str:
    ok = isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
    return format(value, spec) if ok else NA


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return lines + [""]


def _section(title: str, body: Sequence[str]) -> list[str]:
    return [f"## {title}", "", *body]


def _run(run_info: Mapping[str, Any]) -> list[str]:
    parameters = run_info.get("parameters") or {}
    lines = [f"{RUN_DATE_PREFIX}{run_info.get('date', NA)}",
             f"- Tool commit: {run_info.get('tool_commit', NA)}", "- Parameters:"]
    for key in sorted(parameters):
        lines.append(f"  - `{key}`: {json.dumps(parameters[key], sort_keys=True)}")
    lines += ["", "Commands:", "", "```"]
    lines += [str(c) for c in run_info.get("commands") or []]
    return lines + ["```", ""]


def _noise_rows(rows: Sequence[Mapping[str, Any]], sensor: str) -> list[list[str]]:
    out = []
    for r in rows:
        legacy, new = r[f"legacy_{sensor}_density"], r[f"new_{sensor}_density"]
        ratio = legacy / new if legacy is not None and new else None
        out.append([r["mcu_family"], str(r["n_logs"]), _num(legacy), _num(new), _num(ratio, ".3g"),
                    _num(r[f"legacy_{sensor}_per_sample_400hz"]),
                    _num(r[f"new_{sensor}_per_sample_400hz"])])
    return out


def _noise(noise: Sequence[dict]) -> list[str]:
    rows = noise_before_after(noise)
    lines = ["Median noise density at rest per MCU family. The current column is the estimator "
             "TH3SEUS uses today; the new column is the fixed estimator. Per-sample noise is "
             f"density times sqrt({TICK_HZ:g}), the noise one {TICK_HZ:g} Hz tick adds.", ""]
    if not rows:
        return lines + ["No MCU family has enough logs with a noise result.", ""]
    heads = ["MCU family", "Logs", "Current (TH3SEUS estimator)", "New", "Current / new",
             f"Current per sample at {TICK_HZ:g} Hz", f"New per sample at {TICK_HZ:g} Hz"]
    for sensor, label in (("gyro", "Gyro, rad/s/sqrt(Hz); per sample rad/s"),
                          ("accel", "Accelerometer, m/s^2/sqrt(Hz); per sample m/s^2")):
        lines += [f"{label}:", "", *_table(heads, _noise_rows(rows, sensor))]
    return lines


def _battery(battery: Mapping[str, Any]) -> list[str]:
    fits = battery.get("r_fits") or []
    rows = [[f"{f.get('cell_count')}S", _num(f.get("median_mohm_per_cell"), ".2f"),
             _num(f.get("p10_mohm_per_cell"), ".2f"), _num(f.get("p90_mohm_per_cell"), ".2f"),
             _num(f.get("deployed_mohm_per_cell"), ".2f"), str(f.get("n_observations", NA)),
             str(f.get("n_vehicles", NA)), "pooled median" if f.get("low_n") else "own fit"]
            for f in fits]
    lines = ["Internal resistance per cell, from current steps. Deployed is the value the "
             "simulator uses.", "",
             *_table(["Cells", "Median mohm", "p10", "p90", "Deployed mohm", "Steps", "Vehicles",
                      "Deployed from"], rows),
             f"Pooled median: {_num(battery.get('overall_r_median_mohm_per_cell'), '.2f')} "
             f"mohm per cell over {battery.get('overall_r_n', NA)} steps.", ""]
    step = battery.get("step_check") or {}
    met = "met" if battery.get("validation_target_met") else "not met"
    return lines + ["Held-out checks:", "", *_table(["Check", "Value"], [
        ["Held-out logs scored", f"{battery.get('holdout_scored_count', NA)} of "
                                 f"{battery.get('holdout_log_count', NA)}"],
        ["Voltage MAE, median (V per cell)", _num(battery.get("holdout_median_mae_v_per_cell"))],
        ["Voltage MAE, p75 (V per cell)", _num(battery.get("holdout_p75_mae_v_per_cell"))],
        ["Target of 0.1 V per cell", met],
        ["Step check: steps (vehicles)", f"{step.get('n_steps', NA)} ({step.get('n_vehicles', NA)})"],
        ["Step check: median error (V per cell)", _num(step.get("median_err_v_per_cell"))],
        ["Step check: p75 error (V per cell)", _num(step.get("p75_err_v_per_cell"))]])]


def _range_cells(r: RangeRow, points: Sequence[str]) -> list[str]:
    return [r.quantity, str(r.n_logs), str(r.n_vehicles), *(_num(getattr(r, p)) for p in points)]


def _response(ranges: Sequence[RangeRow]) -> list[str]:
    rows = [r for r in ranges if r.class_kind == ALL and r.quantity.startswith(RESPONSE_QUANTITIES)]
    lines = ["Overall distributions over vehicle medians (each vehicle counts once). "
             "Effectiveness is in motor-command space; lag is the body-rate lag minus the "
             "log's filter delay, an upper bound on motor lag.", ""]
    if not rows:
        return lines + ["No response quantity has enough vehicles.", ""]
    return lines + _table(["Quantity", "Logs", "Vehicles", "p5", "p25", "p50", "p75", "p95"],
                          [_range_cells(r, ("p5", "p25", "p50", "p75", "p95")) for r in rows])


def _airframes(airframes: Mapping[str, Any]) -> list[str]:
    lines = [f"A kit is compared when it has at least {airframes.get('min_vehicles', NA)} vehicles. "
             "Ratio is the TH3SEUS prediction over the real median.", ""]
    for c in airframes.get("comparisons") or []:
        lines += [f"### {c.get('kit')} (airframe {c.get('airframe_id')}, "
                  f"{c.get('n_vehicles')} vehicles)", ""]
        if c.get("rows"):
            lines += _table(["Quantity", "Real value", "Predicted", "Real median", "Real p25",
                             "Real p75", "Vehicles", "Ratio"],
                            [[r["quantity"], str(r.get("label") or r["quantity"]),
                              _num(r["predicted"]), _num(r["real_median"]),
                              _num(r["real_p25"]), _num(r["real_p75"]), str(r["n_vehicles"]),
                              _num(r["ratio"], ".3g")] for r in c["rows"]])
        if c.get("note"):
            lines += [str(c["note"]), ""]
    lines += ["Recommendations:", ""]
    for r in airframes.get("recommendations") or []:
        verdict = "change" if r.get("recommended") else "no change"
        lines.append(f"- {r.get('formula')} ({r.get('quantity')}): {verdict}. {r.get('note')}")
    return lines + [""]


def _ranges(ranges: Sequence[RangeRow]) -> list[str]:
    overall = [r for r in ranges if r.class_kind == ALL]
    classes = {r.quantity: sum(1 for x in ranges if x.quantity == r.quantity and x.class_kind != ALL)
               for r in overall}
    lines = ["The full table, with the cell-count and airframe classes, is in `ranges.csv`. "
             "Overall rows:", ""]
    rows = [[*_range_cells(r, ("p5", "p25", "p50", "p75", "p95")), str(classes[r.quantity])]
            for r in overall]
    return lines + _table(["Quantity", "Logs", "Vehicles", "p5", "p25", "p50", "p75", "p95",
                           "Class rows"], rows)


def _row(ranges: Sequence[RangeRow], quantity: str) -> RangeRow | None:
    return next((r for r in ranges if r.quantity == quantity and r.class_kind == ALL), None)


def _vibration(noise: Sequence[dict], response: Sequence[dict]) -> str:
    v = vibration_evidence(noise, response)
    if v is None:
        return "Vibration: not modelled. No log gave both an at-rest and an in-flight gyro noise."
    return ("Vibration: not modelled. In flight, the median gyro noise above the in-flight cut-off "
            f"is {_num(v.median_inflight_std)} rad/s per sample at a median {_num(v.median_rate_hz)}"
            f" Hz. On the density basis (per-sample std times sqrt(1 / rate), which treats the "
            f"signal as white) that is {_num(v.inflight_density)} rad/s/sqrt(Hz), against "
            f"{_num(v.at_rest_density)} at rest: a ratio of {_num(v.ratio, '.3g')} "
            f"({v.n_inflight} flight logs, {v.n_at_rest} at-rest logs). The two bases are "
            "not exactly comparable, so both are given.")


def _esc(ranges: Sequence[RangeRow]) -> str:
    row = _row(ranges, "motor_lag_s")
    base = "ESC dynamics: not modelled beyond a first-order lag with delay."
    if row is None:
        return base + " No motor lag distribution is available."
    return (f"{base} Measured motor lag: median {_num(row.p50)} s, p5 to p95 "
            f"{_num(row.p5)} to {_num(row.p95)} s over {row.n_vehicles} vehicles.")


def _gaps(noise: Sequence[dict], response: Sequence[dict], ranges: Sequence[RangeRow]) -> list[str]:
    items = [_vibration(noise, response),
             "Wind: not modelled. These passes do not measure it.",
             "Ground effect: not modelled. These passes do not measure it.",
             "Motor-to-motor differences: not modelled. The motor lag is a median over the "
             "ESCs of a log, so the spread between motors is not kept.",
             _esc(ranges)]
    return [f"- {item}" for item in items] + [""]


def _counts_block(c: PassCounts) -> list[str]:
    failed = NA if c.failed is None else str(c.failed)
    lines = [f"### {c.name.capitalize()}", "",
             f"Analyzed {c.analyzed}, skipped {c.skipped}, failed {failed}."
             + (" Failed logs are counted with skipped." if c.failed is None else ""), ""]
    if c.reasons:
        lines += _table(["Reason (details removed, numbers as N)", "Logs"],
                        [[reason, str(n)] for reason, n in c.reasons])
    return lines


def _counts(noise: Sequence[dict], battery: Mapping[str, Any],
            response: Sequence[dict]) -> list[str]:
    ids = log_id_lists(noise, battery, response)
    blocks = (entry_counts("noise", noise, ids["noise"]),
              battery_counts(battery, ids["battery"]),
              entry_counts("response", response, ids["response"]))
    return [line for block in blocks for line in _counts_block(block)]


def render_report(noise: Sequence[dict], battery: Mapping[str, Any], response: Sequence[dict],
                  airframes: Mapping[str, Any], ranges: Sequence[RangeRow],
                  run_info: Mapping[str, Any]) -> str:
    """The calibration report; sections in the order of `SECTIONS`."""
    bodies = (_run(run_info), _noise(noise), _battery(battery), _response(ranges),
              _airframes(airframes), _ranges(ranges), _gaps(noise, response, ranges),
              _counts(noise, battery, response))
    lines = ["# TH3SEUS calibration from public PX4 flight logs", ""]
    for title, body in zip(SECTIONS, bodies):
        lines += _section(title, body)
    return "\n".join(lines).rstrip("\n") + "\n"
