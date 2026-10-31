"""Tests for the randomization ranges, the log-ID lists and the calibration directory."""

from __future__ import annotations

import json
import math
from dataclasses import asdict
from pathlib import Path

import pytest

from log_validation import reference_airframes
from log_validation.batch_analyze import batch_analyze
from log_validation.battery_fit import analyze_logs
from log_validation.battery_output import result_to_dict
from log_validation.calibration_render import SECTIONS
from log_validation.calibration_report import (
    RangeRow,
    log_id_lists,
    main,
    noise_before_after,
    ranges_csv,
    ranges_table,
    render_report,
    vehicle_percentiles,
    write_calibration_dir,
)
from log_validation.outputs import write_json
from log_validation.response import response_batch
from log_validation.tests import battery_fakes
from log_validation.tests.calibration_inputs import (
    airframes_dict,
    battery_dict,
    noise_entries,
    response_entries,
    run_info,
    write_inputs,
)
from log_validation.tests.test_batch_analyze import load_by_stem as noise_loader
from log_validation.tests.test_response import load_by_stem as response_loader
from log_validation.th3seus_model import REFERENCE_BUILDS

COMMAND_ARGS = ("noise", "battery", "response", "airframes", "run_info")
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
    assert find(ranges_table([], EMPTY_BATTERY, four), "hover.command").n_vehicles == 4


def test_overall_row_vehicle_weighting():
    logs = [resp(f"a{i}", "v1", **{"hover.command": 0.30}) for i in range(3)]
    logs.append(resp("b0", "v2", **{"hover.command": 0.50}))
    row = find(ranges_table([], EMPTY_BATTERY, logs), "hover.command")
    assert row.n_vehicles == 2 and row.n_logs == 4 and row.p50 == pytest.approx(0.40)


def test_noise_list_has_contributing_logs_only():
    entries = [noise(f"n{i:02d}", f"v{i}", **{"gyro.gyro_noise_density": 1e-3}) for i in range(8)]
    entries += [noise("x1"), noise("x2", **{"gyro.gyro_noise_density": math.nan})]
    lists = log_id_lists(entries[::-1], EMPTY_BATTERY, [])
    assert lists["noise"] == [f"n{i:02d}" for i in range(8)]


def test_noise_list_has_every_entry_behind_a_published_noise_number():
    entries = [noise("legacy_only", **{"gyro.legacy_noise_density": 2e-3}),
               noise("profile_only", **{"baro.noise_sigma": 0.1}),
               noise("new_only", **{"accel.accel_noise_density": 1e-3}),
               noise("none"), noise("nan_only", **{"gyro.legacy_noise_density": math.nan})]
    assert log_id_lists(entries, EMPTY_BATTERY, [])["noise"] == [
        "legacy_only", "new_only", "profile_only"]


def _f7(log_id: str, legacy, new) -> dict:
    measurements = {"gyro.gyro_noise_density": new}
    if legacy is not None:
        measurements["gyro.legacy_noise_density"] = legacy
    return {**noise(log_id, f"v-{log_id}"), "hardware": {"mcu": "STM32F7xx"},
            "measurements": measurements}


def test_before_after_medians_are_paired_per_family():
    entries = [_f7(f"p{i}", 2e-3, 1e-4) for i in range(3)]
    entries += [_f7(f"u{i}", None, 9e-4) for i in range(3)]
    row = next(r for r in noise_before_after(entries) if r["mcu_family"] == "STM32F7")
    assert row["gyro_n_logs"] == 3
    assert row["new_gyro_density"] == pytest.approx(1e-4)
    assert row["legacy_gyro_density"] == pytest.approx(2e-3)
    text = render_report(entries, EMPTY_BATTERY, [], airframes_dict(), [], run_info())
    assert "| STM32F7 | 3 | 0.002 | 0.0001 | 20 |" in text


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
    assert find(rows, "effectiveness.roll").n_logs == 6  # battery-scaled kept, no THR_MDL_FAC not
    scaled_only = [resp("s", "v", **{"effectiveness.roll": 10.0, "hover.thr_mdl_fac": 0.0,
                                     "bat_scale_en": 1.0})]
    assert log_id_lists([], EMPTY_BATTERY, scaled_only)["response"] == ["s"]
    no_fac_only = [resp("n", "v", **{"effectiveness.roll": 10.0})]
    assert log_id_lists([], EMPTY_BATTERY, no_fac_only)["response"] == []


def test_effectiveness_of_a_linear_log_without_a_hover_segment_is_kept():
    logs = [resp(f"l{i}", f"v{i}", **{"effectiveness.roll": 10.0, "param.THR_MDL_FAC": 0.0})
            for i in range(5)]
    assert find(ranges_table([], EMPTY_BATTERY, logs), "effectiveness.roll").n_logs == 5
    assert log_id_lists([], EMPTY_BATTERY, logs)["response"] == [f"l{i}" for i in range(5)]


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


# ---- noise before and after, the report, the directory ----


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


def test_report_labels_the_airframe_lag_as_an_upper_bound():
    kit = reference_airframes.Kit("kit-l", 4001, REFERENCE_BUILDS["impulse_apex_5"], {}, ())
    logs = [resp(f"l{i}", f"v{i}", 4001, **{"lag_minus_filter_s.roll": 0.01}) for i in range(5)]
    comparisons = reference_airframes.compare_kits([kit], logs)
    airframes = {"min_vehicles": 5, "comparisons": [asdict(c) for c in comparisons],
                 "recommendations": [asdict(r) for r in reference_airframes.recommendations(
                     comparisons)]}
    text = render_report([], EMPTY_BATTERY, logs, airframes, [], run_info())
    assert "| lag | body-rate lag minus filter delay (upper bound on motor lag) |" in text


INPUTS = {"manifest_rows": 10, "rows_without_log": 1, "logs_without_row": 2, "capped_out": 3,
          "download_failed": 4}


def _counts_section(info: dict) -> str:
    text = render_report(noise_entries(), battery_dict(), response_entries(), airframes_dict(),
                         [], info)
    return text.split("## Counts")[1]


def test_report_renders_the_input_counts():
    counts = _counts_section({**run_info(), "inputs": INPUTS})
    for line in ("| Manifest rows | 10 |", "| Manifest rows without a log | 1 |",
                 "| Logs without a manifest row | 2 |", "| Logs over the per-vehicle cap | 3 |",
                 "| Failed downloads | 4 |"):
        assert line in counts
    assert counts.index("### Inputs") < counts.index("### Noise")


def test_report_states_that_the_input_counts_are_absent():
    counts = _counts_section(run_info())
    assert "The input counts were not provided" in counts and "| Manifest rows |" not in counts


def test_bad_input_counts_are_refused(tmp_path, capsys):
    paths = write_inputs(tmp_path / "in")
    paths["run_info"].write_text(json.dumps({**run_info(), "inputs": {"capped_out": "3"}}))
    code = main(["--noise", str(paths["noise"]), "--battery", str(paths["battery"]),
                 "--response", str(paths["response"]), "--airframes", str(paths["airframes"]),
                 "--run-info", str(paths["run_info"]), "--out-dir", str(tmp_path / "o")])
    assert code == 2 and "inputs" in capsys.readouterr().err


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


LEAKY = "leaky"


def noise_loader_leaky(path: Path):
    """The leaky log fails to load with a message that holds its path."""
    if path.stem == LEAKY:
        raise OSError(f"cannot open {path}")
    return noise_loader(path)


def response_loader_leaky(path: Path):
    if path.stem == LEAKY:
        raise OSError(f"cannot open {path}")
    return response_loader(path)


def _run_passes(tmp_path):
    """Run the four passes on fake logs, one of which fails to load with its path in the error."""
    ids = [f"{100 + i}" for i in range(10)] + [LEAKY]
    log_dir = battery_fakes.make_log_dir(tmp_path, ids)
    manifest = tmp_path / "m.csv"
    manifest.write_text("log_id,vehicle_id\n" + "".join(f"{i},v{i}\n" for i in ids))
    work = tmp_path / "work"
    forbidden = (str(log_dir.resolve()), str(Path.home()))
    batch_analyze(log_dir, work / "noise.json", manifest_path=manifest, loader=noise_loader_leaky)
    response_batch(log_dir, work / "response.json", manifest_path=manifest,
                   loader=response_loader_leaky)
    fit = analyze_logs(log_dir, manifest_path=manifest, loader=battery_fakes.load_by_stem)
    write_json(work / "battery.json", result_to_dict(fit), forbidden)
    response = str(work / "response.json")
    assert reference_airframes.main([response, "-o", str(work / "airframes.json")]) == 0
    write_json(work / "run-info.json", run_info(), forbidden)
    return log_dir, work


def _calibrate_args(work: Path, out: Path) -> list[str]:
    return ["--noise", str(work / "noise.json"), "--battery", str(work / "battery.json"),
            "--response", str(work / "response.json"), "--airframes", str(work / "airframes.json"),
            "--run-info", str(work / "run-info.json"), "--out-dir", str(out)]


def test_local_path_is_not_leaked(tmp_path):
    log_dir, work = _run_passes(tmp_path)
    noise = json.loads((work / "noise.json").read_text())
    leaky = next(e for e in noise if e["log_id"] == LEAKY)
    assert leaky["errors"] == ["load failed: OSError"]
    out = tmp_path / "out"
    assert main(_calibrate_args(work, out)) == 0
    files = [f for f in out.rglob("*") if f.is_file()]
    assert len(files) == 11
    for f in files:
        text = f.read_text()
        assert str(log_dir.resolve()) not in text and str(Path.home()) not in text
        assert str(work.resolve()) not in text


def test_logs_path_in_run_info_commands_is_refused_with_forbid(tmp_path, capsys):
    log_dir, work = _run_passes(tmp_path)
    info = run_info()
    info["commands"] = [f"python -m log_validation.batch_analyze {log_dir.resolve()}"]
    (work / "run-info.json").write_text(json.dumps(info))
    out = tmp_path / "out"
    code = main([*_calibrate_args(work, out), "--forbid", str(log_dir.resolve())])
    assert code == 2 and "private data" in capsys.readouterr().err
    assert not out.exists()


def test_non_empty_out_dir_is_refused(tmp_path, capsys):
    paths = write_inputs(tmp_path / "in")
    out = tmp_path / "out"
    out.mkdir()
    (out / "stale.txt").write_text("old")
    args = ["--noise", str(paths["noise"]), "--battery", str(paths["battery"]),
            "--response", str(paths["response"]), "--airframes", str(paths["airframes"]),
            "--run-info", str(paths["run_info"]), "--out-dir", str(out)]
    assert main(args) == 2
    assert "not empty" in capsys.readouterr().err
    assert [p.name for p in out.iterdir()] == ["stale.txt"]
    (out / "stale.txt").unlink()
    assert main(args) == 0  # an existing empty directory is fine


def test_a_forbid_string_shorter_than_3_characters_is_refused(tmp_path, capsys):
    paths = write_inputs(tmp_path / "in")
    code = main(["--noise", str(paths["noise"]), "--battery", str(paths["battery"]),
                 "--response", str(paths["response"]), "--airframes", str(paths["airframes"]),
                 "--run-info", str(paths["run_info"]), "--out-dir", str(tmp_path / "o"),
                 "--forbid", "ab"])
    assert code == 2 and "--forbid" in capsys.readouterr().err


def test_vibration_ratio_says_it_is_biased_low():
    from log_validation.calibration_noise import vibration_evidence

    text = render_report(noise_entries(), battery_dict(), response_entries(), airframes_dict(),
                         [], run_info())
    gaps = " ".join(text.split("## Gaps TH3SEUS does not model")[1].split())
    assert "biased low" in gaps and "IMU_GYRO_CUTOFF" in gaps
    doc = " ".join(vibration_evidence.__doc__.split())
    assert "biased low" in doc and "IMU_GYRO_CUTOFF" in doc


def _args(paths: dict[str, Path], out: Path) -> list[str]:
    return ["--noise", str(paths["noise"]), "--battery", str(paths["battery"]),
            "--response", str(paths["response"]), "--airframes", str(paths["airframes"]),
            "--run-info", str(paths["run_info"]), "--out-dir", str(out)]


def _rewrite(path: Path, change) -> None:
    doc = json.loads(path.read_text())
    change(doc)
    path.write_text(json.dumps(doc))


@pytest.mark.parametrize("name, change", [
    ("noise", lambda d: d[0].__setitem__("measurements", ["x"])),
    ("noise", lambda d: d[0].__setitem__("errors", 5)),
    ("response", lambda d: d[0].__setitem__("measurements", "x")),
    ("battery", lambda d: d.__setitem__("per_log_r", 5)),
    ("airframes", lambda d: d.__setitem__("recommendations", [3])),
])
def test_malformed_nested_row_exits_2_without_writing(tmp_path, capsys, name, change):
    paths = write_inputs(tmp_path / "in")
    _rewrite(paths[name], change)
    out = tmp_path / "out"
    assert main(_args(paths, out)) == 2
    last = capsys.readouterr().err.strip().splitlines()[-1]
    assert last.startswith("Error: ") and "malformed" in last
    assert not out.exists()


@pytest.mark.parametrize("name, text, message", [
    ("noise", "{not json", "noise results is not valid JSON"),
    ("response", json.dumps({"log_id": "x"}), "response results must hold a JSON list"),
    ("battery", json.dumps([1, 2]), "battery fit must hold a JSON dict"),
    ("noise", json.dumps([{"measurements": {}}]), "per-log results must be objects with a log_id"),
    ("response", json.dumps([{"log_id": 7}]), "per-log results must be objects with a log_id"),
    ("airframes", json.dumps({"min_vehicles": 5}),
     "airframes JSON lacks comparisons, recommendations"),
])
def test_input_refusals_exit_2(tmp_path, capsys, name, text, message):
    paths = write_inputs(tmp_path / "in")
    paths[name].write_text(text)
    out = tmp_path / "out"
    assert main(_args(paths, out)) == 2
    assert message in capsys.readouterr().err
    assert not out.exists()


def test_out_dir_that_is_a_file_is_refused(tmp_path, capsys):
    paths = write_inputs(tmp_path / "in")
    out = tmp_path / "out"
    out.write_text("a file")
    assert main(_args(paths, out)) == 2
    assert "not empty" in capsys.readouterr().err
    assert out.read_text() == "a file"


@pytest.fixture
def other_identity(monkeypatch):
    """A user and host name no temporary path holds, so only the folder rules can refuse."""
    from log_validation import outputs

    monkeypatch.setattr(outputs.getpass, "getuser", lambda: "zz-test-user-zz")
    monkeypatch.setattr(outputs.socket, "gethostname", lambda: "zz-test-host-zz")


@pytest.mark.parametrize("private", ["input folder", "home"])
def test_input_folder_and_home_are_refused_without_forbid(tmp_path, capsys, other_identity,
                                                         private):
    paths = write_inputs(tmp_path / "in")
    folder = str(paths["noise"].resolve().parent) if private == "input folder" else str(Path.home())
    command = f"python -m log_validation.batch_analyze {folder}"
    _rewrite(paths["run_info"], lambda d: d.__setitem__("commands", [command]))
    out = tmp_path / "out"
    assert main(_args(paths, out)) == 2
    assert "private data" in capsys.readouterr().err
    assert not out.exists()


def test_input_folder_given_through_a_symlink_is_refused(tmp_path, capsys, other_identity):
    real = tmp_path / "real"
    paths = write_inputs(real)
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)
    linked = {k: link / p.name for k, p in paths.items()}
    command = f"python -m log_validation.batch_analyze {link}"
    _rewrite(paths["run_info"], lambda d: d.__setitem__("commands", [command]))
    out = tmp_path / "out"
    assert main(_args(linked, out)) == 2
    assert "private data" in capsys.readouterr().err
    assert not out.exists()


def test_vibration_evidence_numbers():
    from log_validation.calibration_noise import vibration_evidence

    evidence = vibration_evidence(noise_entries(), response_entries())
    # At rest: 1.0e-4 .. 1.5e-4 over six logs, median 1.25e-4. In flight: std 0.02 on
    # every axis at 250 Hz, density 0.02 / sqrt(250).
    assert evidence.at_rest_density == pytest.approx(1.25e-4)
    assert evidence.inflight_density == pytest.approx(0.02 / math.sqrt(250.0))
    assert evidence.ratio == pytest.approx(0.02 / math.sqrt(250.0) / 1.25e-4)
    assert (evidence.n_at_rest, evidence.n_inflight) == (6, 6)
    assert evidence.median_inflight_std == pytest.approx(0.02)
    assert evidence.median_rate_hz == pytest.approx(250.0)


def test_vibration_evidence_uses_the_mean_of_the_axes_and_skips_incomplete_logs():
    from log_validation.calibration_noise import vibration_evidence

    flight = {"inflight_gyro_noise.roll": 0.01, "inflight_gyro_noise.pitch": 0.02,
              "inflight_gyro_noise.yaw": 0.03, "inflight_gyro_rate_hz": 400.0}
    no_yaw = {k: v for k, v in flight.items() if k != "inflight_gyro_noise.yaw"}
    response = [{"log_id": "a", "measurements": flight},
                {"log_id": "b", "measurements": no_yaw},
                {"log_id": "c", "measurements": {**flight, "inflight_gyro_rate_hz": 0.0}}]
    rest = [{"log_id": "n", "measurements": {"gyro.gyro_noise_density": 1e-4}}]
    evidence = vibration_evidence(rest, response)
    assert evidence.n_inflight == 1
    assert evidence.median_inflight_std == pytest.approx(0.02)
    assert evidence.inflight_density == pytest.approx(0.02 / 20.0)


def test_vibration_evidence_is_none_without_both_sides():
    from log_validation.calibration_noise import vibration_evidence

    assert vibration_evidence([], response_entries()) is None
    assert vibration_evidence(noise_entries(), []) is None
    zero_rest = [{"log_id": "n", "measurements": {"gyro.gyro_noise_density": 0.0}}]
    assert vibration_evidence(zero_rest, response_entries()) is None


def test_a_pipe_in_a_reason_does_not_split_its_counts_row():
    noise = noise_entries()
    noise[-1] = {**noise[-1], "errors": ["gyro.x: field 'a|b' not in topic 'sensor_gyro'"]}
    text = render_report(noise, battery_dict(), response_entries(), airframes_dict(), [],
                         run_info())
    row = next(line for line in text.splitlines() if "a\\|b" in line)
    assert row == "| gyro.x: field 'a\\|b' not in topic 'sensor_gyro' | 1 |"


def test_battery_table_shows_why_a_class_falls_back():
    battery = battery_dict()
    own = battery["r_fits"][0]
    floor = {**own, "cell_count": 8, "median_mohm_per_cell": 3.3, "low_n": True,
             "fallback_reason": "implausible (median below the 4 mohm/cell floor)"}
    older = {**own, "cell_count": 6, "low_n": True}  # a JSON written before the reason
    battery["r_fits"] = [own, floor, older]
    text = render_report([], battery, [], airframes_dict(), [], run_info())
    rows = {line.split(" | ")[0]: line for line in text.splitlines() if line.startswith("| ")}
    assert rows["| 4S"].endswith("| own fit |")
    assert rows["| 8S"].endswith(
        "| pooled median: implausible (median below the 4 mohm/cell floor) |")
    assert rows["| 6S"].endswith("| pooled median |")


def test_reference_airframes_section_names_its_logs():
    text = render_report([], battery_dict(), [], airframes_dict(), [], run_info())
    section = " ".join(text.split("## Reference airframes")[1].split("## ")[0].split())
    assert ("Each kit is compared on the logs of `log-ids/response.txt` whose airframe ID "
            "is the kit's.") in section
