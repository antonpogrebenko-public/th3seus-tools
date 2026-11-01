"""The part tables, the board summary and the capacity rows in the calibration report."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from log_validation import part_tables
from log_validation.battery_output import rust_constants_text
from log_validation.calibration_parts import (
    BATTERY_RUST_FILE, PART_TABLES_SECTION, TOP_BOARDS, PartOutputs, load_part_outputs,
    render_part_tables)
from log_validation.calibration_render import SECTIONS, render_report
from log_validation.calibration_report import main, write_calibration_dir
from log_validation.calibration_ranges import ranges_table
from log_validation.outputs import json_text
from log_validation.tests.calibration_inputs import (
    airframes_dict, battery_dict, noise_entries, response_entries, run_info, write_inputs)
from log_validation.tests.part_fakes import noise_entry

ROW = {"part": "imu", "variant": "Icm42688p", "status": "existing", "log_names": ["ICM42688P"],
       "parameter": "gyro_noise_density", "measurement": "gyro.gyro_noise_density",
       "fix_class": None, "logs": 900, "vehicles": 300, "median": 6.1e-5, "p25": 5e-5,
       "p75": 7e-5, "current": 4.88e-5, "proposed": 6.1e-5, "reason": None, "checked": 0,
       "failed_slope": 0, "failed_resolution": 0, "median_rate_hz": None,
       "median_factor": None, "factor_flagged": False}
MAG_ROW = {**ROW, "part": "mag", "variant": "Rm3100", "log_names": ["RM3100"],
           "parameter": "noise_sigma_gauss", "proposed": None,
           "reason": "too few logs pass the white-noise check", "checked": 12,
           "failed_slope": 7, "failed_resolution": 2, "median_rate_hz": 100.0,
           "median_factor": 0.7071}
COMMAND_ARGS = ("noise", "battery", "response", "airframes", "run_info")
IMU_M = {"gyro.gyro_noise_density": 1.234e-3, "accel.accel_noise_density": 2.5e-2}
PART_FILES = {"part-tables.json", "board-table.json", "log-ids/parts.txt", "rust/imu_chip.rs",
              "rust/baro_chip.rs", "rust/mag_chip.rs", "rust/gps_chipset.rs",
              "rust/daemon_aliases.rs", "rust/battery_tables.rs"}


def render(rows, boards=None, parameters=None, **extra):
    tables = {"rows": rows, "log_ids": [], "parameters": parameters or {}, **extra}
    return "\n".join(render_part_tables(tables, {"boards": boards or {}}))


def board(vehicles, imu=None):
    return {"logs": vehicles * 2, "vehicles": vehicles, "imu": imu or {}, "baro": {}, "mag": {},
            "imu_gyro_ratemax_median": 400.0, "imu_gyro_cutoff_median": None}


def _files(directory: Path) -> dict[str, bytes]:
    return {p.relative_to(directory).as_posix(): p.read_bytes()
            for p in sorted(directory.rglob("*")) if p.is_file()}


def part_entries() -> list[dict]:
    """50 BMI088 logs plus one mag-only log that fails the white-noise check."""
    entries = [noise_entry(f"i{n:02d}", f"v{n:02d}", IMU_M, imu="BMI088") for n in range(50)]
    entries.append(noise_entry("m1", "vm", {"mag.noise_sigma_gauss": 1e-3,
                                            "mag.allan_slope": 0.0,
                                            "mag.resolution_gauss": 1e-6,
                                            "mag.rate_hz": 50.0}, mag="RM3100"))
    return entries


def make_parts(tmp_path: Path, entries=None, name="parts") -> Path:
    """Run the part_tables pass on `entries` (`part_entries()` by default)."""
    entries = part_entries() if entries is None else entries
    noise = tmp_path / "pin" / f"{name}-noise.json"
    noise.parent.mkdir(exist_ok=True)
    noise.write_text(json.dumps(entries))
    parts = tmp_path / name
    code = part_tables.main([str(noise), "--out-dir", str(parts), "--run-date", "2026-11-05"])
    assert code == 0
    return parts


def parts_and_inputs(tmp_path: Path, entries=None) -> tuple[Path, dict[str, Path]]:
    """A parts folder and the calibration inputs whose noise results hold its logs."""
    entries = part_entries() if entries is None else entries
    return make_parts(tmp_path, entries), write_inputs(tmp_path / "in", extra_noise=entries)


def cli(paths, out, *extra):
    args = []
    for key in COMMAND_ARGS:
        args += [f"--{key.replace('_', '-')}", str(paths[key])]
    return main([*args, "--out-dir", str(out), *extra])


# ---- the rendering ----


def test_old_and_new_side_by_side():
    text = render([ROW])
    assert ("| Icm42688p (ICM42688P) | gyro_noise_density | existing | 4.88e-05 | 6.1e-05 "
            "| 900 | 300 | 5e-05 | 7e-05 |") in text
    assert "### IMU" in text


def test_missing_proposal_explained():
    text = render([MAG_ROW])
    assert ("too few logs pass the white-noise check (7 failed the slope, 2 the resolution, "
            "of 12 checked)") in text
    assert "| Rm3100 (RM3100) | noise_sigma_gauss | existing | 4.88e-05 | n/a |" in text


def test_a_reason_without_a_white_noise_check_has_no_counts():
    below = {**MAG_ROW, "reason": "below the vehicle threshold", "checked": 0}
    text = render([below])
    assert "| below the vehicle threshold |" in text and "failed the slope" not in text


def test_failure_counts_show_with_any_reason_when_logs_were_checked():
    # Three vehicles whose logs all failed: the reason is the threshold, the counts still show.
    below = {**MAG_ROW, "reason": "below the vehicle threshold", "checked": 3,
             "failed_slope": 3, "failed_resolution": 1}
    text = render([below])
    assert ("| below the vehicle threshold (3 failed the slope, 1 the resolution, "
            "of 3 checked) |") in text


def test_conversion_factor_flag_is_shown():
    # 800 Hz logged, 50 Hz simulator: factor sqrt(50 / 800) = 0.25, below 1/3.
    flagged = {**MAG_ROW, "reason": None, "proposed": 1e-3, "median_rate_hz": 800.0,
               "median_factor": 0.25, "factor_flagged": True}
    plain = {**MAG_ROW, "variant": "Qmc5883l", "log_names": [], "reason": None,
             "proposed": 1e-3, "factor_flagged": False}
    lines = render([flagged, plain]).splitlines()
    assert "Logged rate (Hz) | Factor | Flag" in "\n".join(lines)
    hit = next(line for line in lines if line.startswith("| Rm3100"))
    assert "| 800 | 0.25 | yes (beyond 3x) |" in hit
    other = next(line for line in lines if line.startswith("| Qmc5883l |"))
    assert "yes" not in other and other.endswith("| 100 | 0.7071 |  |")


def test_imu_rows_have_no_rate_columns_and_gps_shows_the_fix_class():
    gps = {**ROW, "part": "gps", "variant": "Ublox", "log_names": [],
           "parameter": "horizontal_accuracy_m", "fix_class": "3d"}
    text = render([ROW, gps])
    assert "Logged rate" not in text.split("### GPS receiver")[0]
    assert "| Ublox | horizontal_accuracy_m (3d) |" in text


def test_entry_column_names_candidates_and_other():
    rows = [{**ROW, "variant": "Newchip", "status": "candidate"},
            {**ROW, "variant": "Other", "status": "other", "log_names": []}]
    text = render(rows)
    assert "| Newchip (ICM42688P) | gyro_noise_density | candidate (new) |" in text
    assert "| Other | gyro_noise_density | Other |" in text


def test_a_pipe_in_a_part_name_does_not_split_the_row():
    text = render([{**ROW, "variant": "A|B"}])
    assert "| A\\|B (ICM42688P) |" in text


def test_a_line_break_in_a_part_name_does_not_split_the_row():
    text = render([{**ROW, "log_names": ["ICM\r\n42688P", "X\rY\nZ"]}])
    assert "| Icm42688p (ICM 42688P, X Y Z) | gyro_noise_density |" in text


def test_a_null_count_reads_as_not_available():
    text = render([{**ROW, "logs": None, "vehicles": None}])
    assert "None" not in text and "| 6.1e-05 | n/a | n/a | 5e-05 |" in text


def test_nan_values_in_a_row_read_as_not_available():
    nan = float("nan")
    text = render([{**MAG_ROW, "median": nan, "p25": nan, "p75": nan, "proposed": nan,
                    "median_rate_hz": nan, "median_factor": nan}])
    assert "nan" not in text.lower()
    assert "| 4.88e-05 | n/a | 900 | 300 | n/a | n/a |" in text


def test_part_type_sections_are_in_order_whatever_the_row_order():
    rows = [{**ROW, "part": part} for part in ("gps", "mag", "baro", "imu")]
    headings = [line for line in render(rows).splitlines() if line.startswith("### ")]
    assert headings == ["### IMU", "### Barometer", "### Magnetometer", "### GPS receiver",
                        "### Boards"]


def test_the_parameters_line_and_the_missing_versions():
    full = render([ROW], parameters={"min_vehicles": 10, "allan_slope_tolerance": 0.15,
                                     "sim_rate_baro_hz": 50.0, "sim_rate_mag_hz": 50.0},
                  hitl_physics_version="0.13.1", hitl_daemon_version="0.27.0")
    assert "hitl-physics 0.13.1" in full and "hitl-daemon 0.27.0" in full
    assert "10 vehicles" in full and "0.15" in full
    bare = render([ROW])
    assert "hitl-physics n/a" in bare and "hitl-daemon n/a" in bare


def test_empty_part_type_says_so():
    assert "No rows." in render([ROW]).split("### Barometer")[1].split("### Magnetometer")[0]


def test_boards_with_the_most_vehicles_and_the_board_file():
    boards = {f"B{i:02d}": board(i + 1, {"BMI088": 0.7, "ICM42688P": 0.3} if i == 11 else None)
              for i in range(12)}
    boards["A_TIE"] = board(12)
    text = render([ROW], boards)
    section = text.split("### Boards")[1]
    names = [m.group(1) for line in section.splitlines()
             if (m := re.match(r"\| (B\d\d|A_TIE) ", line))]
    assert TOP_BOARDS == 10 and len(names) == 10
    assert names == ["A_TIE", "B11", "B10", "B09", "B08", "B07", "B06", "B05", "B04", "B03"]
    assert "| B11 | 24 | 12 | BMI088 0.70, ICM42688P 0.30 |" in section
    assert "Board | Logs | Vehicles | IMU | Baro | Mag | IMU_GYRO_RATEMAX | IMU_GYRO_CUTOFF" in text
    assert "`board-table.json`" in text


def test_report_places_the_section_after_the_noise_section():
    noise, response = noise_entries(), response_entries()
    ranges = ranges_table(noise, battery_dict(), response)
    args = (noise, battery_dict(), response, airframes_dict(), ranges, run_info())
    with_parts = render_report(*args, parts={"rows": [ROW], "log_ids": []}, boards={"boards": {}})
    headings = [line[3:] for line in with_parts.splitlines() if line.startswith("## ")]
    assert headings[:3] == ["Run", "IMU noise before and after", PART_TABLES_SECTION]
    assert headings[3:] == list(SECTIONS[2:]) and PART_TABLES_SECTION == "Part tables"
    plain = render_report(*args)
    assert [ln[3:] for ln in plain.splitlines() if ln.startswith("## ")] == list(SECTIONS)


def _battery_part(battery) -> str:
    return render_report([], battery, [], airframes_dict(), [], run_info()).split("## Battery")[1]


def test_battery_section_has_the_capacity_rows():
    text = render_report(noise_entries(), battery_dict(), response_entries(), airframes_dict(),
                         [], run_info())
    battery = text.split("## Battery")[1].split("## Vehicle response")[0]
    assert "| 4S | 5000-10000 | 9.50 | 7.50 | 12.00 | 120 | 6 |" in battery
    assert "Cells | Capacity (mAh) | Median mohm | p10 | p90 | Steps | Vehicles" in battery
    assert "5000, 10000" in battery


def test_old_battery_json_without_capacity_rows_still_renders():
    old = {k: v for k, v in battery_dict().items()
           if k not in ("r_fits_by_capacity", "capacity_bounds_mah")}
    text = _battery_part(old)
    assert "no capacity rows" in text.lower() and "Capacity (mAh)" not in text
    assert "| 4S | 9.50 |" in text


def test_empty_capacity_rows_say_no_class_has_5_vehicles():
    empty = {**battery_dict(), "r_fits_by_capacity": []}
    text = _battery_part(empty)
    assert "5 vehicles" in text and "Capacity (mAh)" not in text


# ---- loading ----


def test_load_part_outputs_reads_every_file(tmp_path):
    out = load_part_outputs(make_parts(tmp_path))
    assert isinstance(out, PartOutputs) and len(out.tables["log_ids"]) == 50
    assert "boards" in out.boards
    assert set(out.rust) == {"imu_chip.rs", "baro_chip.rs", "mag_chip.rs", "gps_chipset.rs",
                             "daemon_aliases.rs"}


def _break(parts: Path, name: str, text: str | None) -> None:
    target = parts / name
    target.unlink() if text is None else target.write_text(text, encoding="utf-8")


BAD_PARTS = [
    ("part-tables.json", None, "part-tables.json"),
    ("part-tables.json", "[]", "part-tables.json"),
    ("part-tables.json", "not json", "part-tables.json"),
    ("part-tables.json", '{"rows": {}, "log_ids": []}', "rows"),
    ("part-tables.json", '{"rows": [1], "log_ids": []}', "rows"),
    ("part-tables.json", '{"rows": [], "log_ids": [1]}', "log_ids"),
    ("part-tables.json", '{"rows": []}', "log_ids"),
    ("board-table.json", None, "board-table.json"),
    ("board-table.json", '{"boards": []}', "boards"),
    ("board-table.json", "{}", "boards"),
    ("rust/imu_chip.rs", None, "imu_chip.rs"),
    ("rust/imu_chip.rs", "// caf\u00e9\n", "ASCII"),
]


@pytest.mark.parametrize("name,text,message", BAD_PARTS)
def test_malformed_parts_dir_is_refused(tmp_path, name, text, message):
    parts = make_parts(tmp_path)
    _break(parts, name, text)
    with pytest.raises(ValueError, match=message):
        load_part_outputs(parts)


def test_missing_parts_dir_is_refused(tmp_path):
    with pytest.raises(ValueError):
        load_part_outputs(tmp_path / "absent")


# ---- the directory ----


def test_only_contributing_logs(tmp_path):
    parts, paths = parts_and_inputs(tmp_path)
    out = tmp_path / "out"
    write_calibration_dir(out, *(paths[k] for k in COMMAND_ARGS), parts_dir=parts)
    ids = (out / "log-ids" / "parts.txt").read_text().splitlines()
    assert len(ids) == 50 and "m1" not in ids and ids == sorted(set(ids))


def test_log_ids_are_sorted_and_without_duplicates(tmp_path):
    parts, paths = parts_and_inputs(tmp_path)
    doc = json.loads((parts / "part-tables.json").read_text())
    doc["log_ids"] = ["i10", "i02", "i10", "i05"]
    (parts / "part-tables.json").write_text(json.dumps(doc))
    out = tmp_path / "out"
    write_calibration_dir(out, *(paths[k] for k in COMMAND_ARGS), parts_dir=parts)
    assert (out / "log-ids" / "parts.txt").read_text() == "i02\ni05\ni10\n"


def test_directory_with_parts_has_every_file(tmp_path):
    parts, paths = parts_and_inputs(tmp_path)
    out = tmp_path / "out"
    assert cli(paths, out, "--parts", str(parts)) == 0
    names = set(_files(out))
    assert names >= PART_FILES and len(names) == 12 + len(PART_FILES) - 1
    assert (out / "rust" / BATTERY_RUST_FILE).read_text() == rust_constants_text(
        battery_dict()) + "\n"
    for name in ("board-table.json", "rust/imu_chip.rs", "rust/daemon_aliases.rs"):
        assert (out / name).read_text() == (parts / name).read_text()
    report = (out / "report.md").read_text()
    assert "## Part tables" in report and "BMI088" in report


def test_the_copied_part_tables_leave_the_log_ids_to_parts_txt(tmp_path):
    parts, paths = parts_and_inputs(tmp_path)
    out = tmp_path / "out"
    assert cli(paths, out, "--parts", str(parts)) == 0
    source = json.loads((parts / "part-tables.json").read_text())
    copied_text = (out / "part-tables.json").read_text()
    assert "log_ids" in source and "log_ids" not in json.loads(copied_text)
    assert copied_text == json_text({k: v for k, v in source.items() if k != "log_ids"})
    ids = (out / "log-ids" / "parts.txt").read_text().splitlines()
    assert ids == sorted(source["log_ids"]) and len(ids) == 50


def test_parts_from_other_noise_results_are_refused_without_writing(tmp_path, capsys):
    parts = make_parts(tmp_path)
    paths = write_inputs(tmp_path / "in", extra_noise=part_entries()[:49])  # no "i49"
    out = tmp_path / "out"
    assert cli(paths, out, "--parts", str(parts)) == 2
    err = capsys.readouterr().err
    assert err.startswith("Error: ") and "not from these noise results" in err
    assert "1 log ID" in err and "i49" in err
    assert not out.exists() or not any(out.iterdir())


def test_parts_with_no_proposed_value_pass_the_log_id_check(tmp_path):
    parts, paths = parts_and_inputs(tmp_path, [noise_entry("solo", "v1", IMU_M, imu="BMI088")])
    out = tmp_path / "out"
    assert cli(paths, out, "--parts", str(parts)) == 0
    assert (out / "log-ids" / "parts.txt").read_text() == ""


def test_directory_without_parts_has_the_battery_rust_only(tmp_path):
    paths = write_inputs(tmp_path / "in")
    out = tmp_path / "out"
    assert cli(paths, out) == 0
    names = set(_files(out))
    assert "rust/battery_tables.rs" in names and len(names) == 12
    assert not names & {"part-tables.json", "board-table.json", "log-ids/parts.txt"}
    assert "## Part tables" not in (out / "report.md").read_text()


@pytest.mark.parametrize("name,text", [
    ("part-tables.json", "[]"), ("part-tables.json", None), ("board-table.json", "{}"),
    ("rust/mag_chip.rs", "// caf\u00e9\n")])
def test_malformed_parts_dir_exits_2_without_writing(tmp_path, capsys, name, text):
    parts, paths = parts_and_inputs(tmp_path)
    _break(parts, name, text)
    out = tmp_path / "out"
    assert cli(paths, out, "--parts", str(parts)) == 2
    assert capsys.readouterr().err.startswith("Error: ")
    assert not out.exists() or not any(out.iterdir())


def test_a_private_string_in_a_parts_file_is_refused_without_writing(tmp_path, capsys):
    parts, paths = parts_and_inputs(tmp_path)
    leaky = parts / "rust" / "imu_chip.rs"
    leaky.write_text(f"// built in {parts.resolve()}\n", encoding="utf-8")
    out = tmp_path / "out"
    assert cli(paths, out, "--parts", str(parts)) == 2
    assert "private data" in capsys.readouterr().err
    assert not out.exists() or not any(out.iterdir())


def test_a_forbid_string_applies_to_the_parts_files(tmp_path, capsys):
    parts, paths = parts_and_inputs(tmp_path)
    (parts / "rust" / "imu_chip.rs").write_text("// acme-private-bucket-123\n", encoding="utf-8")
    out = tmp_path / "out"
    assert cli(paths, out, "--parts", str(parts), "--forbid", "acme-private-bucket-123") == 2
    assert "private data" in capsys.readouterr().err
    assert not out.exists() or not any(out.iterdir())


def test_parts_rerun_is_identical(tmp_path):
    parts, paths = parts_and_inputs(tmp_path)
    first, second = tmp_path / "one", tmp_path / "two"
    assert cli(paths, first, "--parts", str(parts)) == 0
    assert cli(paths, second, "--parts", str(parts)) == 0
    assert _files(first) == _files(second)
