"""The part_tables pass: inputs, outputs and errors."""

from __future__ import annotations

import gzip
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from log_validation.part_tables import (
    check_noise_entries, load_catalog_spellings, main, part_tables_document)
from log_validation.part_rows import RowRules, build_part_tables
from log_validation.tests.part_fakes import noise_entry

OUTPUTS = {"part-tables.json", "board-table.json", "rust/imu_chip.rs", "rust/baro_chip.rs",
           "rust/mag_chip.rs", "rust/gps_chipset.rs", "rust/daemon_aliases.rs"}
IMU_M = {"gyro.gyro_noise_density": 1.234e-3, "accel.accel_noise_density": 2.5e-2}
REPO_ROOT = Path(__file__).resolve().parents[2]


def entries_of_12_vehicles(name="BMI088", n=12):
    return [noise_entry(f"l{i}", f"v{i}", IMU_M, imu=name) for i in range(n)]


def two_spellings(n=24):
    return [noise_entry(f"l{i}", f"v{i}", IMU_M, imu=("BMI088" if i % 2 else "bmi088"))
            for i in range(n)]


def files(root: Path) -> set[str]:
    return {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}


def write_input(tmp_path, entries):
    noise = tmp_path / "in" / "noise.json"
    noise.parent.mkdir(exist_ok=True)
    noise.write_text(entries if isinstance(entries, str) else json.dumps(entries))
    return noise


def run(tmp_path, entries, *extra, out="out", date="2026-11-05"):
    noise = write_input(tmp_path, entries)
    return main([str(noise), "--out-dir", str(tmp_path / out), "--run-date", date, *extra])


def nothing_written(tmp_path, out="out"):
    return not (tmp_path / out).exists() or not any((tmp_path / out).iterdir())


def test_malformed_input_exits_2_and_writes_nothing(tmp_path):
    assert run(tmp_path, {"a": 1}) == 2
    assert nothing_written(tmp_path)


@pytest.mark.parametrize("text", [
    "not json", "[1, 2]", '[{"log_id": 3}]',
    '[{"log_id": "a", "measurements": [], "errors": []}]'])
def test_unreadable_or_malformed_input_exits_2(tmp_path, text, capsys):
    assert run(tmp_path, text) == 2
    assert nothing_written(tmp_path)
    assert capsys.readouterr().err.startswith("Error: ")


def test_missing_input_exits_2(tmp_path):
    code = main([str(tmp_path / "absent.json"), "--out-dir", str(tmp_path / "out"),
                 "--run-date", "2026-11-05"])
    assert code == 2
    assert nothing_written(tmp_path)


def test_writes_every_output(tmp_path):
    assert run(tmp_path, entries_of_12_vehicles()) == 0
    assert files(tmp_path / "out") == OUTPUTS
    doc = json.loads((tmp_path / "out" / "part-tables.json").read_text())
    assert doc["parameters"] == {"min_vehicles": 10, "allan_slope_tolerance": 0.15,
                                 "sim_rate_baro_hz": 50.0, "sim_rate_mag_hz": 50.0}
    assert (doc["hitl_physics_version"], doc["hitl_daemon_version"], doc["run_date"]) == \
        ("0.13.1", "0.27.0", "2026-11-05")
    assert {"part", "variant", "proposed", "current", "vehicles"} <= set(doc["rows"][0])
    assert isinstance(doc["log_ids"], list)
    board = json.loads((tmp_path / "out" / "board-table.json").read_text())
    assert board == {"boards": {"PX4_FMU_V5": board["boards"]["PX4_FMU_V5"]}}
    assert "Bmi088" in (tmp_path / "out" / "rust" / "imu_chip.rs").read_text()


def test_every_output_is_ascii(tmp_path):
    assert run(tmp_path, entries_of_12_vehicles()) == 0
    for name in OUTPUTS:
        (tmp_path / "out" / name).read_bytes().decode("ascii")


def test_options_reach_the_document(tmp_path):
    assert run(tmp_path, entries_of_12_vehicles(), "--min-vehicles", "3",
               "--allan-slope-tolerance", "0.2", "--sim-rate-baro", "100",
               "--sim-rate-mag", "25") == 0
    doc = json.loads((tmp_path / "out" / "part-tables.json").read_text())
    assert doc["parameters"] == {"min_vehicles": 3, "allan_slope_tolerance": 0.2,
                                 "sim_rate_baro_hz": 100.0, "sim_rate_mag_hz": 25.0}


def test_document_matches_the_tables():
    rules = RowRules()
    tables = build_part_tables(entries_of_12_vehicles())
    doc = part_tables_document(tables, rules, "2026-11-05")
    assert set(doc) == {"hitl_physics_version", "hitl_daemon_version", "run_date",
                        "parameters", "rows", "log_ids"}
    assert len(doc["rows"]) == len(tables.rows)
    assert doc["log_ids"] == list(tables.log_ids)
    json.dumps(doc, allow_nan=False)


def _run_in_subprocess(noise, out, seed, *extra):
    env = {**os.environ, "PYTHONHASHSEED": str(seed)}
    done = subprocess.run(
        [sys.executable, "-m", "log_validation.part_tables", str(noise), "--out-dir", str(out),
         "--run-date", "2026-11-05", *extra],
        cwd=REPO_ROOT, env=env, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr


def test_rerun_is_byte_identical(tmp_path):
    noise = write_input(tmp_path, two_spellings())
    catalog = tmp_path / "in" / "catalog.json"
    catalog.write_text(json.dumps(
        {"imu": {"BMI-088": "BMI088", "Bosch BMI088": "bmi088", "bmi_088": "BMI088"}}))
    for seed, out in ((0, "a"), (1, "b")):
        _run_in_subprocess(noise, tmp_path / out, seed, "--catalog-spellings", str(catalog))
    assert files(tmp_path / "a") == files(tmp_path / "b") == OUTPUTS
    for name in OUTPUTS:
        assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes()
    aliases = (tmp_path / "a" / "rust" / "daemon_aliases.rs").read_text()
    assert '"bmi088"' in aliases and "ImuChip::Bmi088" in aliases


def test_private_string_is_refused_and_nothing_written(tmp_path, capsys):
    entries = entries_of_12_vehicles(n=10)
    entries[0] = {**entries[0], "log_id": str(tmp_path / "in")}
    assert run(tmp_path, entries) == 2
    assert nothing_written(tmp_path)
    assert "private data" in capsys.readouterr().err


@pytest.mark.parametrize("extra", [["--min-vehicles", "0"], ["--allan-slope-tolerance", "0"],
                                   ["--allan-slope-tolerance", "-1"],
                                   ["--sim-rate-mag", "0"], ["--sim-rate-baro", "-5"]])
def test_bad_options_exit_2(tmp_path, extra, capsys):
    assert run(tmp_path, entries_of_12_vehicles(), *extra) == 2
    assert nothing_written(tmp_path)
    assert capsys.readouterr().err.startswith("Error: ")


@pytest.mark.parametrize("date", ["2026/11/05", "2026-11-5", "x2026-11-05", "2026-11-05\n"])
def test_bad_run_date_exits_2(tmp_path, date):
    assert run(tmp_path, entries_of_12_vehicles(), date=date) == 2
    assert nothing_written(tmp_path)


@pytest.mark.parametrize("catalog", ['{"imu": ["BMI088"]}', '{"wheel": {}}', '{"imu": {"X": 1}}',
                                     "[]", "not json"])
def test_malformed_catalog_spellings_exit_2(tmp_path, catalog):
    path = tmp_path / "in" / "catalog.json"
    write_input(tmp_path, entries_of_12_vehicles())
    path.write_text(catalog)
    assert run(tmp_path, entries_of_12_vehicles(), "--catalog-spellings", str(path)) == 2
    assert nothing_written(tmp_path)


def test_missing_catalog_spellings_file_exits_2(tmp_path):
    assert run(tmp_path, entries_of_12_vehicles(),
               "--catalog-spellings", str(tmp_path / "none.json")) == 2
    assert nothing_written(tmp_path)


def test_catalog_spellings_reach_the_aliases(tmp_path):
    write_input(tmp_path, entries_of_12_vehicles())
    path = tmp_path / "in" / "catalog.json"
    path.write_text(json.dumps({"imu": {"BMI-088": "BMI088"}}))
    assert run(tmp_path, entries_of_12_vehicles(), "--catalog-spellings", str(path)) == 0
    aliases = (tmp_path / "out" / "rust" / "daemon_aliases.rs").read_text()
    lines = [ln for ln in aliases.splitlines() if '"bmi088" =>' in ln]
    assert len(lines) == 1 and lines[0].count('"bmi088"') == 1


def test_non_empty_out_dir_is_refused(tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    (out / "stale.txt").write_text("keep")
    assert run(tmp_path, entries_of_12_vehicles()) == 2
    assert [p.name for p in out.iterdir()] == ["stale.txt"]


def test_empty_out_dir_is_accepted(tmp_path):
    (tmp_path / "out").mkdir()
    assert run(tmp_path, entries_of_12_vehicles()) == 0
    assert files(tmp_path / "out") == OUTPUTS


def test_invalid_variant_identifier_exits_2(tmp_path, capsys):
    assert run(tmp_path, entries_of_12_vehicles(name="123")) == 2
    assert nothing_written(tmp_path)
    assert "Rust identifier" in capsys.readouterr().err


def test_empty_list_is_valid_input(tmp_path):
    assert run(tmp_path, []) == 0
    assert files(tmp_path / "out") == OUTPUTS


# check_noise_entries

def good():
    return noise_entry("a", "v", IMU_M, imu="BMI088")


def test_check_returns_the_entries():
    entries = [good()]
    assert check_noise_entries(entries) == entries


@pytest.mark.parametrize("value", [{}, "x", None, [1], [[]], [{"measurements": {}}]])
def test_check_rejects_bad_shapes(value):
    with pytest.raises(ValueError):
        check_noise_entries(value)


@pytest.mark.parametrize("change", [
    {"log_id": 3}, {"measurements": []}, {"measurements": None},
    {"parts": []}, {"parts": {"imu": 3}}, {"parts": {"imu": []}},
    {"vehicle_id": 3}, {"hardware": []}, {"hardware": "x"}, {"errors": {}}, {"errors": "x"},
    {"parts": {"imu": {"name": 3}}}, {"parts": {"imu": {"name": []}}},
])
def test_check_rejects_bad_fields(change):
    with pytest.raises(ValueError):
        check_noise_entries([{**good(), **change}])


@pytest.mark.parametrize("change", [
    {"vehicle_id": None}, {"vehicle_id": "v"}, {"hardware": {}}, {"errors": []},
    {"parts": {}}, {"parts": {"imu": {"name": None}}}, {"parts": {"imu": {"name": "X"}}}])
def test_check_accepts_good_fields(change):
    check_noise_entries([{**good(), **change}])


def test_check_accepts_absent_optional_fields():
    entry = {"log_id": "a", "measurements": {}}
    assert check_noise_entries([entry]) == [entry]


# load_catalog_spellings

def test_load_catalog_spellings(tmp_path):
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"imu": {"BMI-088": "BMI088"}, "gps": {}}))
    assert load_catalog_spellings(path) == {"imu": {"BMI-088": "BMI088"}, "gps": {}}


def test_load_catalog_spellings_unreadable(tmp_path):
    with pytest.raises(OSError):
        load_catalog_spellings(tmp_path / "none.json")


def test_part_tables_reads_a_gzip_input(tmp_path):
    entries = entries_of_12_vehicles()
    assert run(tmp_path, entries, out="plain") == 0
    packed = tmp_path / "in" / "noise.json.gz"
    packed.write_bytes(gzip.compress(json.dumps(entries).encode("utf-8")))
    code = main([str(packed), "--out-dir", str(tmp_path / "gz"), "--run-date", "2026-11-05"])
    assert code == 0
    for name in OUTPUTS:
        assert (tmp_path / "gz" / name).read_bytes() == (tmp_path / "plain" / name).read_bytes()


@pytest.mark.parametrize("data", [
    gzip.compress(json.dumps([1] * 500).encode())[:-15], b"plain text, not gzip"])
def test_corrupt_gzip_input_exits_2_and_writes_nothing(tmp_path, data, capsys):
    bad = tmp_path / "in" / "noise.json.gz"
    bad.parent.mkdir()
    bad.write_bytes(data)
    code = main([str(bad), "--out-dir", str(tmp_path / "out"), "--run-date", "2026-11-05"])
    assert code == 2
    assert nothing_written(tmp_path)
    assert capsys.readouterr().err.startswith("Error: ")
