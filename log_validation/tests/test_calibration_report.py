"""Tests for the randomization ranges and the log-ID lists."""

from __future__ import annotations

import math

import pytest

from log_validation.calibration_report import (
    RangeRow,
    log_id_lists,
    ranges_csv,
    ranges_table,
    vehicle_percentiles,
)

EMPTY_BATTERY: dict = {"per_log_r": [], "fit_log_ids": [], "holdout_scored_log_ids": []}


def resp(log_id, vehicle_id=None, airframe_id=None, **measurements) -> dict:
    return {"log_id": log_id, "vehicle_id": vehicle_id, "airframe_id": airframe_id,
            "hardware": {}, "measurements": dict(measurements), "errors": []}


def noise(log_id, vehicle_id=None, airframe_id=None, **measurements) -> dict:
    return resp(log_id, vehicle_id, airframe_id, **measurements)


def find(rows, quantity, kind="all", value="all"):
    hits = [r for r in rows if (r.quantity, r.class_kind, r.class_value) == (quantity, kind, value)]
    return hits[0] if hits else None


def test_vehicle_weighting():
    n_logs, n_veh, p = vehicle_percentiles(
        [("a", "v1", .30), ("b", "v1", .30), ("c", "v1", .30), ("d", "v2", .50)])
    assert (n_logs, n_veh) == (4, 2) and p[50] == pytest.approx(0.40)


def test_missing_vehicle_id_is_its_own_vehicle_and_cannot_collide():
    n_logs, n_veh, _ = vehicle_percentiles([("a", None, 1.0), ("b", None, 2.0), ("c", "a", 3.0)])
    assert (n_logs, n_veh) == (3, 3)


def test_class_with_fewer_than_5_vehicles_is_omitted():
    four = [resp(f"l{i}", f"v{i}", 4019, **{"hover.command": 0.4}) for i in range(4)]
    assert find(ranges_table([], EMPTY_BATTERY, four), "hover.command", "airframe", "4019") is None
    five = four + [resp("l4", "v4", 4019, **{"hover.command": 0.4})]
    rows = ranges_table([], EMPTY_BATTERY, five)
    assert find(rows, "hover.command", "airframe", "4019").n_vehicles == 5
    assert find(rows, "hover.command") is not None
    assert find(ranges_table([], EMPTY_BATTERY, four), "hover.command") is None


def test_noise_list_has_contributing_logs_only():
    entries = [noise(f"n{i:02d}", f"v{i}", **{"gyro.gyro_noise_density": 1e-3}) for i in range(8)]
    entries += [noise("x1"), noise("x2", **{"gyro.gyro_noise_density": math.nan})]
    lists = log_id_lists(entries[::-1], EMPTY_BATTERY, [])
    assert lists["noise"] == [f"n{i:02d}" for i in range(8)]


def test_lag_quantity_uses_lag_minus_filter():
    logs = [resp(f"l{i}", f"v{i}", **{"lag_minus_filter_s.roll": 0.01, "lag_s.roll": 0.5})
            for i in range(5)]
    rows = ranges_table([], EMPTY_BATTERY, logs)
    assert find(rows, "lag_minus_filter_s.roll").p50 == pytest.approx(0.01)
    assert not [r for r in rows if r.quantity.startswith("lag_s")]


def test_effectiveness_converted_to_command_space():
    logs = [resp(f"l{i}", f"v{i}", **{"effectiveness.roll": 10.0, "hover.thr_mdl_fac": 0.5,
                                      "hover.command": 0.4}) for i in range(5)]
    row = find(ranges_table([], EMPTY_BATTERY, logs), "effectiveness.roll")
    assert row.p50 == pytest.approx(10.0 * (0.5 + 2 * 0.5 * 0.4))


def test_effectiveness_exclusions():
    base = {"effectiveness.roll": 10.0, "hover.thr_mdl_fac": 0.0, "hover.command": 0.4}
    logs = [resp(f"l{i}", f"v{i}", **base) for i in range(5)]
    logs.append(resp("scaled", "vs", **base, bat_scale_en=1.0))
    logs.append(resp("nofac", "vn", **{"effectiveness.roll": 10.0, "hover.command": 0.4}))
    rows = ranges_table([], EMPTY_BATTERY, logs)
    assert find(rows, "effectiveness.roll").n_logs == 5
    lists = log_id_lists([], EMPTY_BATTERY, logs)
    assert "scaled" in lists["response"]  # contributes hover.command
    only = [resp("s", "v", **{"effectiveness.roll": 10.0, "hover.thr_mdl_fac": 0.0,
                              "hover.command": 0.4, "bat_scale_en": 1.0})]
    only = [{**only[0], "measurements": {k: v for k, v in only[0]["measurements"].items()
                                         if k != "hover.command"}}]
    assert log_id_lists([], EMPTY_BATTERY, only)["response"] == []


def test_response_list_only_ranges_table_quantities():
    logs = [resp("rate_only", "v1", **{"rate.torque_setpoint_hz": 250.0}),
            resp("other", "v2", **{"motor_lag_r2": 0.9, "rate.angular_velocity_hz": 100.0}),
            resp("empty", "v3")]
    assert log_id_lists([], EMPTY_BATTERY, logs)["response"] == ["rate_only"]
    five = [resp(f"r{i}", f"v{i}", **{"rate.torque_setpoint_hz": 250.0}) for i in range(5)]
    assert find(ranges_table([], EMPTY_BATTERY, five), "rate.torque_setpoint_hz") is not None


def test_battery_list_is_fit_plus_scored_holdout():
    battery = {"per_log_r": [], "fit_log_ids": ["b", "a"],
               "holdout_log_ids": ["z", "c", "d"], "holdout_scored_log_ids": ["d", "a", "c"]}
    assert log_id_lists([], battery, [])["battery"] == ["a", "b", "c", "d"]


def test_battery_rows_and_classes():
    per = [{"log_id": f"l{i}", "vehicle_id": f"v{i}", "cell_count": 4,
            "median_mohm_per_cell": 10.0 + i} for i in range(5)]
    rows = ranges_table([], {**EMPTY_BATTERY, "per_log_r": per}, [])
    assert find(rows, "battery.r_mohm_per_cell").p50 == pytest.approx(12.0)
    assert find(rows, "battery.r_mohm_per_cell", "cell_count", "4").n_vehicles == 5
    assert not [r for r in rows if r.class_kind == "airframe"]


def test_non_finite_values_are_ignored():
    n_logs, n_veh, p = vehicle_percentiles(
        [("a", "v1", 1.0), ("b", "v2", math.nan), ("c", "v3", math.inf), ("d", "v4", 3.0)])
    assert (n_logs, n_veh) == (2, 2) and p[50] == pytest.approx(2.0)


def test_csv_is_deterministic_and_sorted():
    logs = [resp(f"l{i}", f"v{i}", 4019, **{"hover.command": 0.3 + i / 100,
                                           "motor_lag_s": 0.05, "cell_count": 4.0})
            for i in range(6)]
    first = ranges_csv(ranges_table([], EMPTY_BATTERY, logs))
    second = ranges_csv(ranges_table([], EMPTY_BATTERY, list(reversed(logs))))
    assert first == second
    lines = first.splitlines()
    assert lines[0] == "quantity,class_kind,class_value,n_logs,n_vehicles,p5,p25,p50,p75,p95"
    keys = [tuple(line.split(",")[:3]) for line in lines[1:]]
    assert keys == sorted(keys) and ("hover.command", "airframe", "4019") in keys
    assert ("hover.command", "cell_count", "4") in keys


def test_range_row_is_frozen():
    row = RangeRow("q", "all", "all", 1, 1, 0.0, 0.0, 0.0, 0.0, 0.0)
    with pytest.raises(Exception):
        row.n_logs = 2  # type: ignore[misc]
