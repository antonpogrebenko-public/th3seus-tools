"""Battery resistance per cell count and pack-capacity class."""

from __future__ import annotations

import argparse
import dataclasses
import json
import math
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from log_validation import battery_fit
from log_validation.battery_capacity import (
    DEFAULT_CAPACITY_BOUNDS_MAH,
    UNKNOWN_CAPACITY,
    capacity_class,
    capacity_class_fits,
    pack_capacity_mah,
    parse_bounds,
)
from log_validation.battery_extract import load_extracted, save_extracted
from log_validation.battery_fit import fit_extracted, split_logs
from log_validation.battery_output import (
    emit_rust_constants,
    result_to_dict,
    rust_constants_text,
    write_markdown_summary,
)
from log_validation.battery_extract import extract_dir
from log_validation.tests.battery_fakes import (
    CORPUS, R_3S_MOHM, load_by_stem, make_data, make_log_dir)

CAPACITY_MAH = 5200.0
CLASS_5000 = "5000-10000"


def obs(log_id, capacity, cells=4, r=(9.0, 10.0)):
    return SimpleNamespace(
        log_id=log_id, cell_count=cells, capacity_mah=capacity, r_obs=list(r)
    )


def vehicles_of(n):
    return {f"l{i}": f"v{i}" for i in range(n)}


def test_capacity_class_row():
    rows = capacity_class_fits([obs(f"l{i}", 5200.0) for i in range(6)], vehicles_of(6))
    assert [(r.cell_count, r.capacity_class, r.n_vehicles, r.n_steps) for r in rows] == [
        (4, CLASS_5000, 6, 12)
    ]
    assert rows[0].p10_mohm_per_cell <= rows[0].median_mohm_per_cell <= rows[0].p90_mohm_per_cell


def test_unset_capacity():
    rows = capacity_class_fits([obs(f"l{i}", -1.0) for i in range(5)], vehicles_of(5))
    assert [r.capacity_class for r in rows] == ["unknown capacity"]


def test_class_with_fewer_than_5_vehicles_is_omitted():
    assert capacity_class_fits([obs(f"l{i}", 5200.0) for i in range(4)], vehicles_of(4)) == ()


def test_vehicles_not_logs_are_counted():
    same_vehicle = {f"l{i}": "v0" for i in range(6)}
    assert capacity_class_fits([obs(f"l{i}", 5200.0) for i in range(6)], same_vehicle) == ()


def test_log_without_a_vehicle_counts_as_its_own():
    rows = capacity_class_fits([obs(f"l{i}", 5200.0) for i in range(5)], {})
    assert rows[0].n_vehicles == 5


def test_logs_without_steps_are_ignored():
    rows = capacity_class_fits(
        [obs(f"l{i}", 5200.0) for i in range(5)] + [obs("none", 5200.0, r=())], vehicles_of(5)
    )
    assert rows[0].n_vehicles == 5 and rows[0].n_steps == 10


def test_class_bounds():
    got = [capacity_class(c, DEFAULT_CAPACITY_BOUNDS_MAH) for c in (800, 1000, 5200, 20000, None, 0)]
    assert got == ["<1000", "1000-2500", "5000-10000", ">=20000", "unknown capacity", "unknown capacity"]
    assert capacity_class(float("nan"), DEFAULT_CAPACITY_BOUNDS_MAH) == UNKNOWN_CAPACITY
    assert capacity_class(-1.0, DEFAULT_CAPACITY_BOUNDS_MAH) == UNKNOWN_CAPACITY
    assert capacity_class(2500.0, (1000.0, 2500.5)) == "1000-2500.5"


def test_rows_sorted_by_cells_then_class_with_unknown_last():
    spec = [(4, -1.0), (4, 5200.0), (4, 800.0), (3, 5200.0)]
    observations = [
        obs(f"{k}-{i}", cap, cells) for k, (cells, cap) in enumerate(spec) for i in range(5)
    ]
    vehicle_of = {o.log_id: o.log_id for o in observations}
    rows = capacity_class_fits(observations, vehicle_of)
    assert [(r.cell_count, r.capacity_class) for r in rows] == [
        (3, CLASS_5000), (4, "<1000"), (4, CLASS_5000), (4, UNKNOWN_CAPACITY),
    ]
    assert all(math.isfinite(v) for r in rows for v in (
        r.median_mohm_per_cell, r.p10_mohm_per_cell, r.p90_mohm_per_cell))


def test_custom_min_vehicles_and_bounds():
    rows = capacity_class_fits(
        [obs(f"l{i}", 300.0) for i in range(2)], vehicles_of(2), bounds=(500.0,), min_vehicles=2
    )
    assert [r.capacity_class for r in rows] == ["<500"]


def test_parse_bounds_rejects_unordered():
    for text in ("2500,1000", "0,10", "5,5", "a,b", "", "1,nan", "1,inf"):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_bounds(text)
    assert parse_bounds("1000, 2500") == (1000.0, 2500.0)


def test_pack_capacity_prefers_bat1():
    assert pack_capacity_mah({"BAT1_CAPACITY": 5200, "BAT_CAPACITY": 3000}) == 5200.0
    assert pack_capacity_mah({"BAT_CAPACITY": 3000}) == 3000.0 and pack_capacity_mah({}) is None
    assert pack_capacity_mah({"BAT1_CAPACITY": -1}) == -1.0
    assert pack_capacity_mah({"BAT1_CAPACITY": float("nan"), "BAT_CAPACITY": 3000}) == 3000.0
    assert pack_capacity_mah({"BAT1_CAPACITY": "x"}) is None


@pytest.mark.parametrize("unset", [-1, 0])
def test_a_logged_unset_bat1_capacity_wins_over_bat_capacity(unset):
    capacity = pack_capacity_mah({"BAT1_CAPACITY": unset, "BAT_CAPACITY": 5000})
    assert capacity == float(unset)
    assert capacity_class(capacity, DEFAULT_CAPACITY_BOUNDS_MAH) == UNKNOWN_CAPACITY


def test_extract_keeps_the_capacity(tmp_path):
    data = dataclasses.replace(make_data("a1"), capacity_mah=CAPACITY_MAH)
    assert load_extracted(save_extracted(data, tmp_path / "x")).capacity_mah == CAPACITY_MAH
    plain = make_data("a2")
    assert plain.capacity_mah is None
    path = save_extracted(plain, tmp_path / "y")
    assert load_extracted(path).capacity_mah is None


def test_old_extract_without_the_key_loads_none(tmp_path):
    path = save_extracted(dataclasses.replace(make_data("a1"), capacity_mah=1.0), tmp_path)
    with np.load(path) as f:
        kept = {k: f[k] for k in f.files if k != "capacity_mah"}
    np.savez_compressed(path, **kept)
    assert load_extracted(path).capacity_mah is None


def test_logged_unset_capacity_round_trips(tmp_path):
    data = dataclasses.replace(make_data("a1"), capacity_mah=-1.0)
    assert load_extracted(save_extracted(data, tmp_path)).capacity_mah == -1.0
    nan = dataclasses.replace(make_data("a2"), capacity_mah=float("nan"))
    assert math.isnan(load_extracted(save_extracted(nan, tmp_path)).capacity_mah)


def test_extract_reads_the_parameter():
    from log_validation.battery_extract import extract_log
    from log_validation.tests.battery_fakes import REAL_INFO, battery_status
    from log_validation.tests.fakes import FakeULog

    ulog = FakeULog(
        {"battery_status": battery_status(4, 9.0, 1)},
        params={"BAT1_CAPACITY": 5200}, info=REAL_INFO,
    )
    status, data = extract_log(Path("z.ulg"), loader=lambda _p: ulog)
    assert status == "ok" and data.capacity_mah == 5200.0


def fitted(capacity=CAPACITY_MAH, capacity_bounds=DEFAULT_CAPACITY_BOUNDS_MAH):
    r_of = {"a1": 8.0, "a2": 8.5, "b1": 9.0, "c1": 10.0, "d1": 11.0, "e1": 12.0, "f1": 9.5}
    vehicle_of = {"a1": "v1", "a2": "v1", "b1": "v2", "c1": "v3", "d1": "v4",
                  "e1": "v5", "f1": "v6", "h1": "vh"}
    datas = [dataclasses.replace(make_data(i, 4, r), capacity_mah=capacity)
             for i, r in r_of.items()]
    datas.append(make_data("h1", 3, R_3S_MOHM))
    seed = next(s for s in range(100)
                if split_logs(list(vehicle_of), vehicle_of, 0.2, s)[1] == ["h1"])
    return fit_extracted(datas, vehicle_of, 0.2, seed, capacity_bounds=capacity_bounds)


def test_fit_reports_capacity_rows():
    result = fitted()
    row = result.r_fits_by_capacity[0]
    assert (row.cell_count, row.capacity_class, row.n_vehicles) == (4, CLASS_5000, 6)
    assert len(result.r_fits_by_capacity) == 1
    assert result.capacity_bounds_mah == DEFAULT_CAPACITY_BOUNDS_MAH
    d = result_to_dict(result)
    assert d["r_fits_by_capacity"][0]["capacity_class"] == CLASS_5000
    assert d["capacity_bounds_mah"] == list(DEFAULT_CAPACITY_BOUNDS_MAH)
    json.dumps(d, allow_nan=False)


def test_fit_with_other_bounds():
    result = fitted(capacity_bounds=(6000.0,))
    assert result.capacity_bounds_mah == (6000.0,)
    assert result.r_fits_by_capacity[0].capacity_class == "<6000"


def test_capacity_bounds_option_reaches_the_outputs(tmp_path):
    obs = tmp_path / "obs"
    extract_dir(make_log_dir(tmp_path, CORPUS), obs, loader=load_by_stem)
    out_json, out_md = tmp_path / "fit.json", tmp_path / "fit.md"
    code = battery_fit.main(["--obs-dir", str(obs), "--out-json", str(out_json),
                             "--out-md", str(out_md), "--capacity-bounds", "6000,12000"])
    assert code in (0, 1)  # 1: the held-out target was missed, both files written
    assert json.loads(out_json.read_text())["capacity_bounds_mah"] == [6000.0, 12000.0]
    assert "class bounds (mAh): 6000, 12000." in out_md.read_text()


def test_markdown_has_the_capacity_table(tmp_path):
    out = tmp_path / "fit.md"
    write_markdown_summary(fitted(), out)
    text = out.read_text()
    assert "## Internal Resistance by Pack Capacity" in text
    assert "| 4S | 5000-10000 |" in text
    assert ("| Cell count | Capacity (mAh) | Median (mohm/cell) | p10 | p90 | n step events "
            "| Vehicles |") in text
    assert ("Pack capacity is the log's battery capacity parameter; class bounds (mAh): "
            "1000, 2500, 5000, 10000, 20000.  A class with fewer than 5 vehicles is left out.  "
            "Spread only: the Rust constants stay per cell count.") in text
    text.encode("ascii")


def test_markdown_without_rows_says_so(tmp_path):
    out = tmp_path / "fit.md"
    write_markdown_summary(dataclasses.replace(fitted(), r_fits_by_capacity=()), out)
    assert "No cell count and capacity class" in out.read_text()


def test_rust_constants_unchanged_in_form():
    result = fitted()
    text = emit_rust_constants(result)
    assert text == emit_rust_constants(dataclasses.replace(result, r_fits_by_capacity=()))
    assert text == rust_constants_text(result_to_dict(result))
    assert "const FITTED_R_TABLE: &[(u8, f64)] = &[" in text and "const OCV_TABLE: &[(f64, f64)] = &[" in text
    assert "5000-10000" not in text


def test_bad_capacity_bounds_exit_2(tmp_path):
    with pytest.raises(SystemExit) as e:
        battery_fit.main(["--capacity-bounds", "5,1", "--log-dir", str(tmp_path)])
    assert e.value.code == 2
