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


# ---- Task 12: noise before and after, the report, the directory ----

import json  # noqa: E402
from pathlib import Path  # noqa: E402

from log_validation.batch_analyze import batch_analyze  # noqa: E402
from log_validation.battery_fit import analyze_logs  # noqa: E402
from log_validation.battery_output import result_to_dict  # noqa: E402
from log_validation import reference_airframes  # noqa: E402
from log_validation.calibration_report import (  # noqa: E402
    main,
    noise_before_after,
    render_report,
    write_calibration_dir,
)
from log_validation.calibration_render import SECTIONS  # noqa: E402
from log_validation.outputs import write_json  # noqa: E402
from log_validation.response import response_batch  # noqa: E402
from log_validation.tests import battery_fakes  # noqa: E402
from log_validation.tests.calibration_inputs import (  # noqa: E402
    airframes_dict,
    battery_dict,
    noise_entries,
    response_entries,
    run_info,
    write_inputs,
)
from log_validation.tests.test_batch_analyze import load_by_stem as noise_loader  # noqa: E402
from log_validation.tests.test_response import load_by_stem as response_loader  # noqa: E402

COMMAND_ARGS = ("noise", "battery", "response", "airframes", "run_info")


def _write(tmp_path, out_name="out", date="2026-10-29", extra=()):
    paths = write_inputs(tmp_path / "in", date)
    out = tmp_path / out_name
    write_calibration_dir(out, *(paths[k] for k in COMMAND_ARGS), extra_forbidden=extra)
    return out, paths


def _files(directory: Path) -> dict[str, bytes]:
    return {str(p.relative_to(directory)): p.read_bytes()
            for p in sorted(directory.rglob("*")) if p.is_file()}


def test_noise_before_and_after():
    rows = noise_before_after(noise_entries())
    row = next(r for r in rows if r["mcu_family"] == "STM32F7")
    assert row["new_gyro_per_sample_400hz"] == pytest.approx(row["new_gyro_density"] * 20.0)
    assert row["legacy_accel_per_sample_400hz"] == pytest.approx(2e-2 * 20.0)
    assert {"legacy_gyro_density", "new_gyro_density", "legacy_gyro_per_sample_400hz"} <= set(row)
    assert [r["mcu_family"] for r in rows] == ["STM32F7", "STM32H7", "ALL"]


def test_report_sections_in_order():
    from log_validation.calibration_report import ranges_table

    noise, response = noise_entries(), response_entries()
    ranges = ranges_table(noise, battery_dict(), response)
    text = render_report(noise, battery_dict(), response, airframes_dict(), ranges, run_info())
    headings = [line[3:] for line in text.splitlines() if line.startswith("## ")]
    assert headings == list(SECTIONS) and len(SECTIONS) == 8
    assert "Current (TH3SEUS estimator)" in text and "kit-a" in text
    assert "Step check: median error" in text and "ratio of" in text
    assert "roll: low excitation | 2" in text


def test_directory_has_every_listed_file(tmp_path):
    out, _ = _write(tmp_path)
    names = set(_files(out))
    assert names == {"report.md", "ranges.csv", "log-ids/noise.txt", "log-ids/battery.txt",
                     "log-ids/response.txt", "noise-results.json", "noise-profiles.json",
                     "battery-fit.json", "response-results.json", "airframes.json",
                     "run-info.json"}
    assert (out / "log-ids" / "battery.txt").read_text() == "b0\nb1\nb2\nb3\nb4\nb5\nh1\nh2\n"
    assert json.loads((out / "run-info.json").read_text())["tool_commit"] == "abc1234"


def test_rerun_is_identical(tmp_path):
    first, _ = _write(tmp_path, "one")
    second, _ = _write(tmp_path, "two")
    assert _files(first) == _files(second)


def test_only_the_run_date_differs_between_dates(tmp_path):
    first, _ = _write(tmp_path, "one", date="2026-10-29")
    second, _ = _write(tmp_path, "two", date="2026-11-30")
    a, b = _files(first), _files(second)
    differing = {k for k in a if a[k] != b[k]}
    assert differing == {"report.md", "run-info.json"}
    lines_a = a["report.md"].decode().splitlines()
    lines_b = b["report.md"].decode().splitlines()
    changed = [x for x, y in zip(lines_a, lines_b) if x != y]
    assert len(lines_a) == len(lines_b) and changed == ["- Run date: 2026-10-29"]


def test_forbidden_string_in_an_input_is_refused_with_exit_2(tmp_path, capsys):
    paths = write_inputs(tmp_path / "in")
    bad = run_info()
    bad["commands"] = ["aws s3 sync s3://acme-private-bucket-123 logs"]
    paths["run_info"].write_text(json.dumps(bad))
    out = tmp_path / "out"
    code = main(["--noise", str(paths["noise"]), "--battery", str(paths["battery"]),
                 "--response", str(paths["response"]), "--airframes", str(paths["airframes"]),
                 "--run-info", str(paths["run_info"]), "--out-dir", str(out),
                 "--forbid", "acme-private-bucket-123"])
    assert code == 2
    assert "private data" in capsys.readouterr().err
    assert not out.exists()


def test_cli_round_trip_writes_every_file(tmp_path):
    paths = write_inputs(tmp_path / "in")
    out = tmp_path / "out"
    code = main(["--noise", str(paths["noise"]), "--battery", str(paths["battery"]),
                 "--response", str(paths["response"]), "--airframes", str(paths["airframes"]),
                 "--run-info", str(paths["run_info"]), "--out-dir", str(out)])
    assert code == 0 and len(_files(out)) == 11
    assert out.joinpath("report.md").read_text().startswith("# ")


def test_bad_input_shape_exits_2(tmp_path, capsys):
    paths = write_inputs(tmp_path / "in")
    paths["run_info"].write_text(json.dumps({"date": "x"}))
    code = main(["--noise", str(paths["noise"]), "--battery", str(paths["battery"]),
                 "--response", str(paths["response"]), "--airframes", str(paths["airframes"]),
                 "--run-info", str(paths["run_info"]), "--out-dir", str(tmp_path / "o")])
    assert code == 2 and "run-info" in capsys.readouterr().err


def test_local_path_is_not_leaked(tmp_path):
    ids = [f"{100 + i}" for i in range(10)]
    log_dir = battery_fakes.make_log_dir(tmp_path, ids)
    manifest = tmp_path / "m.csv"
    manifest.write_text("log_id,vehicle_id\n" + "".join(f"{i},v{i}\n" for i in ids))
    work = tmp_path / "work"
    forbidden = (str(log_dir.resolve()), str(Path.home()))

    batch_analyze(log_dir, work / "noise.json", manifest_path=manifest, loader=noise_loader)
    response_batch(log_dir, work / "response.json", manifest_path=manifest, loader=response_loader)
    fit = analyze_logs(log_dir, manifest_path=manifest, loader=battery_fakes.load_by_stem)
    write_json(work / "battery.json", result_to_dict(fit), forbidden)
    assert reference_airframes.main([str(work / "response.json"), "-o", str(work / "airframes.json")]) == 0
    write_json(work / "run-info.json", run_info(), forbidden)

    out = tmp_path / "out"
    write_calibration_dir(out, work / "noise.json", work / "battery.json", work / "response.json",
                          work / "airframes.json", work / "run-info.json")
    files = [f for f in out.rglob("*") if f.is_file()]
    assert len(files) == 11
    for f in files:
        text = f.read_text()
        assert str(log_dir.resolve()) not in text and str(Path.home()) not in text
        assert str(work.resolve()) not in text


def test_a_forbid_string_shorter_than_3_characters_is_refused(tmp_path, capsys):
    paths = write_inputs(tmp_path / "in")
    code = main(["--noise", str(paths["noise"]), "--battery", str(paths["battery"]),
                 "--response", str(paths["response"]), "--airframes", str(paths["airframes"]),
                 "--run-info", str(paths["run_info"]), "--out-dir", str(tmp_path / "o"),
                 "--forbid", "ab"])
    assert code == 2 and "--forbid" in capsys.readouterr().err
