"""Tests for batch_analyze: result shape, simulation skip, vehicle cap, parallel jobs."""

from __future__ import annotations

import json
import os
from pathlib import Path

from log_validation.batch_analyze import batch_analyze
from log_validation.tests.fakes import FakeULog
from log_validation.tests.test_pipeline import _build_log

SEED_HARDWARE_FIELDS = ["fc_board", "fc_board_subtype", "mcu", "px4_version",
                        "imu_name", "imu_devtype", "mag_name", "baro_name"]
SPI_ICM20689_GYRO_ID = 0x3C000A
AIRFRAME = 4001


def _fake(board: str = "PX4_FMU_V5", autostart: float = AIRFRAME) -> FakeULog:
    base = _build_log()
    return FakeULog(
        {d.name: d.data for d in base.data_list},
        params={"CAL_GYRO0_ID": SPI_ICM20689_GYRO_ID, "SYS_AUTOSTART": autostart},
        info={"ver_hw": board},
    )


class _NoParams:
    """A parse result without the header attributes."""


def load_by_stem(path: Path):
    """Top-level so a spawned worker can import it."""
    if path.stem == "bad":
        raise OSError("corrupt log")
    if path.stem == "die":
        os._exit(1)
    if path.stem == "noparams":
        return _NoParams()
    if path.stem.startswith("sitl"):
        return _fake(board="PX4_SITL")
    return _fake()


def _touch(tmp_path: Path, stems) -> Path:
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    for s in stems:
        (log_dir / f"{s}.ulg").write_bytes(b"")
    return log_dir


def _read(out: Path) -> list[dict]:
    return json.loads(out.read_text())


def test_entry_has_the_seeding_fields(tmp_path):
    log_dir = _touch(tmp_path, ["a1"])
    out = tmp_path / "out" / "r.json"
    batch_analyze(log_dir, out, loader=load_by_stem)
    entry = _read(out)[0]
    assert set(SEED_HARDWARE_FIELDS) <= set(entry["hardware"])
    assert {"log_id", "measurements", "errors", "vehicle_id", "airframe_id"} <= set(entry)
    assert "gyro.gyro_noise_density" in entry["measurements"]
    assert entry["airframe_id"] == AIRFRAME
    assert entry["measurements"]["gyro.integration_interval_s"] > 0
    assert entry["measurements"]["accel.integration_interval_s"] > 0
    assert entry["vehicle_id"] is None


def test_airframe_id_is_none_when_zero_or_absent(tmp_path):
    log_dir = _touch(tmp_path, ["a1"])
    out = tmp_path / "r.json"
    results = batch_analyze(log_dir, out, loader=lambda p: _fake(autostart=0))
    assert results[0].airframe_id is None


def test_sitl_log_is_skipped(tmp_path):
    log_dir = _touch(tmp_path, ["sitl1"])
    out = tmp_path / "r.json"
    batch_analyze(log_dir, out, loader=load_by_stem)
    entry = _read(out)[0]
    assert entry["measurements"] == {} and any("simulation" in e for e in entry["errors"])


def test_cap_applies(tmp_path):
    ids = ["10", "11", "12", "13", "14", "15"]
    log_dir = _touch(tmp_path, ids)
    manifest = tmp_path / "m.csv"
    manifest.write_text("log_id,vehicle_id\n" + "".join(f"{i},v1\n" for i in ids))
    out = tmp_path / "r.json"
    batch_analyze(log_dir, out, manifest_path=manifest, max_per_vehicle=3, loader=load_by_stem)
    entries = _read(out)
    assert [e["log_id"] for e in entries] == ["10", "11", "12"]
    assert {e["vehicle_id"] for e in entries} == {"v1"}


def test_selection_counts_go_to_stderr(tmp_path, capsys):
    log_dir = _touch(tmp_path, ["a1", "a2", "a3", "a4", "u1"])
    manifest = tmp_path / "m.csv"
    manifest.write_text("log_id,vehicle_id\na1,v1\na2,v1\na3,v1\na4,v1\ngone,v2\n")
    batch_analyze(log_dir, None, manifest_path=manifest, max_per_vehicle=3, loader=load_by_stem)
    err = capsys.readouterr().err
    assert "manifest_rows_without_log: 1" in err
    assert "logs_without_manifest_row: 1" in err and "capped_out: 1" in err


def test_corrupt_log_does_not_stop_the_run(tmp_path):
    log_dir = _touch(tmp_path, ["a1", "bad", "c3"])
    out = tmp_path / "r.json"
    batch_analyze(log_dir, out, jobs=2, loader=load_by_stem)
    by_id = {e["log_id"]: e for e in _read(out)}
    assert any("load failed" in e for e in by_id["bad"]["errors"])
    assert by_id["a1"]["measurements"] and by_id["c3"]["measurements"]


def test_jobs_do_not_change_the_output(tmp_path):
    log_dir = _touch(tmp_path, ["c3", "a1", "bad", "b2"])
    one, two = tmp_path / "one.json", tmp_path / "two.json"
    batch_analyze(log_dir, one, jobs=1, loader=load_by_stem)
    batch_analyze(log_dir, two, jobs=2, loader=load_by_stem)
    assert one.read_bytes() == two.read_bytes()
    assert [e["log_id"] for e in _read(one)] == ["a1", "b2", "bad", "c3"]


def test_log_without_header_attributes_does_not_stop_the_run(tmp_path):
    log_dir = _touch(tmp_path, ["a1", "noparams"])
    out = tmp_path / "r.json"
    batch_analyze(log_dir, out, jobs=2, loader=load_by_stem)
    by_id = {e["log_id"]: e for e in _read(out)}
    assert by_id["noparams"]["airframe_id"] is None and by_id["noparams"]["errors"]
    assert by_id["a1"]["measurements"]


def test_worker_death_is_recorded_and_others_survive(tmp_path):
    log_dir = _touch(tmp_path, ["a1", "die", "c3"])
    out = tmp_path / "r.json"
    batch_analyze(log_dir, out, jobs=2, loader=load_by_stem)
    by_id = {e["log_id"]: e for e in _read(out)}
    assert by_id["die"]["errors"] == ["worker process died"]
    assert by_id["die"]["measurements"] == {} and by_id["die"]["hardware"] == {}
    assert by_id["a1"]["measurements"] and by_id["c3"]["measurements"]


def test_analysis_failure_entry_has_the_exception_type_only(tmp_path, monkeypatch, capsys):
    import log_validation.report as report

    def broken(_ulog):
        raise RuntimeError("secret detail at /some/private/path")

    monkeypatch.setattr(report, "analyze", broken)
    log_dir = _touch(tmp_path, ["a1"])
    results = batch_analyze(log_dir, None, loader=load_by_stem)
    assert results[0].errors == ["analysis failed: RuntimeError"]
    err = capsys.readouterr().err
    assert "a1" in err and "RuntimeError" in err and "secret detail" in err


def test_load_failure_entry_has_the_exception_type_only(tmp_path, capsys):
    def loader(path):
        raise OSError(f"cannot open {path}")

    log_dir = _touch(tmp_path, ["a1"])
    results = batch_analyze(log_dir, None, loader=loader)
    assert results[0].errors == ["load failed: OSError"]
    err = capsys.readouterr().err
    assert "a1" in err and "OSError" in err and str(log_dir) in err


def test_hardware_failure_entry_has_the_exception_type_only(tmp_path, monkeypatch, capsys):
    import log_validation.batch_analyze as module

    def broken(_info, _params):
        raise KeyError("/private/place")

    monkeypatch.setattr(module, "extract_hardware_info", broken)
    log_dir = _touch(tmp_path, ["a1"])
    results = batch_analyze(log_dir, None, loader=load_by_stem)
    assert "hardware extraction failed: KeyError" in results[0].errors
    assert not any("private" in e for e in results[0].errors)
    assert "/private/place" in capsys.readouterr().err


def test_parameters_failure_entry_has_the_exception_type_only(tmp_path, capsys):
    log_dir = _touch(tmp_path, ["noparams"])
    results = batch_analyze(log_dir, None, loader=load_by_stem)
    assert "parameters unavailable: AttributeError" in results[0].errors
    assert "noparams" in capsys.readouterr().err
