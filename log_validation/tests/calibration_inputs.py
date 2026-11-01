"""Small synthetic pass outputs for the calibration report tests."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

N_VEHICLES = 6


def noise_entries() -> list[dict]:
    entries = []
    for i in range(N_VEHICLES):
        entries.append({
            "log_id": f"n{i:02d}", "vehicle_id": f"v{i}", "airframe_id": 4001,
            "hardware": {"mcu": "STM32F7xx" if i % 2 else "STM32H7xx"},
            "measurements": {"gyro.gyro_noise_density": 1e-4 * (1 + i / 10),
                             "gyro.legacy_noise_density": 2e-3, "accel.accel_noise_density": 1e-3,
                             "accel.legacy_noise_density": 2e-2},
            "errors": []})
    entries.append({"log_id": "sim1", "vehicle_id": None, "airframe_id": None, "hardware": {},
                    "measurements": {}, "errors": ["simulation log skipped (SITL/HIL)"]})
    entries.append({"log_id": "bad1", "vehicle_id": None, "airframe_id": None, "hardware": {},
                    "measurements": {}, "errors": ["load failed: corrupt log"]})
    return entries


def response_entries() -> list[dict]:
    entries = []
    for i in range(N_VEHICLES):
        entries.append({
            "log_id": f"r{i:02d}", "vehicle_id": f"v{i}", "airframe_id": 4001, "hardware": {},
            "measurements": {"hover.command": 0.4, "motor_lag_s": 0.05,
                             "effectiveness.roll": 20.0, "lag_minus_filter_s.roll": 0.02,
                             "inflight_gyro_noise.roll": 0.02, "inflight_gyro_noise.pitch": 0.02,
                             "inflight_gyro_noise.yaw": 0.02, "inflight_gyro_rate_hz": 250.0,
                             "cell_count": 4.0},
            "errors": []})
    for i, std in enumerate((0.001, 0.003)):
        entries.append({"log_id": f"w{i}", "vehicle_id": None, "airframe_id": None, "hardware": {},
                        "measurements": {}, "errors": [f"roll: low excitation (std {std} < 0.02)"]})
    return entries


def battery_dict() -> dict:
    per_log = [{"log_id": f"b{i}", "vehicle_id": f"v{i}", "cell_count": 4,
                "median_mohm_per_cell": 9.0 + i} for i in range(N_VEHICLES)]
    return {
        "real_log_count": 8, "sim_log_count": 1, "skipped_log_count": 2,
        "logs_without_manifest_row": 0, "split_seed": 42,
        "r_fits": [{"cell_count": 4, "median_mohm_per_cell": 9.5, "p10_mohm_per_cell": 7.0,
                    "p25_mohm_per_cell": 8.0, "p75_mohm_per_cell": 11.0, "p90_mohm_per_cell": 13.0,
                    "n_observations": 300, "n_vehicles": 6, "deployed_mohm_per_cell": 9.5,
                    "low_n": False}],
        "capacity_bounds_mah": [5000.0, 10000.0],
        "r_fits_by_capacity": [{"cell_count": 4, "capacity_class": "5000-10000", "n_steps": 120,
                                "n_vehicles": 6, "median_mohm_per_cell": 9.5,
                                "p10_mohm_per_cell": 7.5, "p90_mohm_per_cell": 12.0}],
        "ocv_curve": {"soc_knots": [0.0, 0.5, 1.0], "v_per_cell_knots": [3.3, 3.8, 4.2]},
        "overall_r_median_mohm_per_cell": 9.5, "overall_r_n": 300,
        "holdout_log_count": 3, "holdout_scored_count": 2,
        "holdout_median_mae_v_per_cell": 0.03, "holdout_mean_mae_v_per_cell": 0.04,
        "holdout_p75_mae_v_per_cell": 0.05, "validation_target_met": True,
        "per_log_r": per_log, "fit_log_ids": [f"b{i}" for i in range(N_VEHICLES)],
        "holdout_log_ids": ["h1", "h2", "h3"], "holdout_scored_log_ids": ["h1", "h2"],
        "fit_logs_without_samples": 0, "holdout_logs_not_scored": 1,
        "step_check": {"n_steps": 40, "n_vehicles": 4, "median_err_v_per_cell": 0.02,
                       "p75_err_v_per_cell": 0.04},
    }


def airframes_dict() -> dict:
    row = {"quantity": "hover.command", "predicted": 0.45, "real_median": 0.4, "real_p25": 0.38,
           "real_p75": 0.42, "n_vehicles": 6, "ratio": 1.125}
    return {"min_vehicles": 5,
            "comparisons": [{"kit": "kit-a", "airframe_id": 4001, "n_vehicles": 6,
                             "compared": True, "rows": [row], "note": "compared"}],
            "recommendations": [{"quantity": "hover.command", "formula": "hover", "recommended": False,
                                 "kits": ["kit-a"], "note": "no change recommended"}]}


def run_info(date: str = "2026-10-29") -> dict:
    return {"date": date, "tool_commit": "abc1234",
            "parameters": {"max_per_vehicle": 3, "gates": {"min_r2": 0.05}, "split_seed": 42},
            "commands": ["python -m log_validation.batch_analyze logs -o noise.json"]}


def write_inputs(directory: Path, run_date: str = "2026-10-29",
                 extra_noise: Sequence[dict] = ()) -> dict[str, Path]:
    """The five inputs as JSON files; `extra_noise` entries follow the noise entries."""
    directory.mkdir(parents=True, exist_ok=True)
    docs = {"noise": noise_entries() + list(extra_noise), "battery": battery_dict(),
            "response": response_entries(), "airframes": airframes_dict(),
            "run_info": run_info(run_date)}
    paths = {}
    for name, doc in docs.items():
        paths[name] = directory / f"{name}.json"
        paths[name].write_text(json.dumps(doc), encoding="utf-8")
    return paths
