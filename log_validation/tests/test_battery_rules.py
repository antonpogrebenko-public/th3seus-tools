"""Battery fit rules: which logs are left out and how they are counted."""

from __future__ import annotations

from dataclasses import replace

import numpy as np
import pytest

import json

from log_validation.battery_fit import (
    _LogObservations,
    _r_table,
    cell_count_matches,
    fit_extracted,
    holdout_by_cell_count,
)
from log_validation.battery_model import OcvCurve
from log_validation.battery_output import result_to_dict, rust_constants_text, write_markdown_summary
from log_validation.tests.battery_fakes import R_3S_MOHM, make_data
from log_validation.tests.calibration_inputs import battery_dict


@pytest.mark.parametrize("cells, volts, ok", [
    (1, 16.8, False), (4, 15.2, True), (4, 12.0, True), (4, 17.6, True),
    (4, 11.99, False), (4, 17.61, False), (0, 15.2, False)])
def test_cell_count_matches(cells, volts, ok):
    d = make_data("x")
    assert cell_count_matches(replace(d, cell_count=cells, v=np.full(d.v.size, volts))) is ok


def test_no_finite_voltage_is_a_mismatch():
    d = make_data("x")
    assert cell_count_matches(replace(d, v=np.full(d.v.size, np.nan))) is False


def test_pack_logged_as_1s_is_left_out(tmp_path):
    datas = [make_data(f"a{k}") for k in range(8)] + [replace(make_data("m1"), cell_count=1)]
    r = fit_extracted(datas, {}, 0.2, 0)
    assert r.cell_count_mismatch_count == 1
    assert "m1" not in r.fit_log_ids and "m1" not in r.holdout_scored_log_ids
    assert all(f.cell_count != 1 for f in r.r_fits)
    assert r.real_log_count == 9 and r.skipped_log_count == 0
    assert result_to_dict(r)["cell_count_mismatch_count"] == 1
    out = tmp_path / "s.md"
    write_markdown_summary(r, out)
    assert "cell count does not match the pack voltage" in out.read_text()


def test_mismatched_log_keeps_the_split():
    base = [make_data(f"a{k}") for k in range(8)]
    with_data = fit_extracted(base + [replace(make_data("m1"), cell_count=1)], {}, 0.2, 0)
    without = fit_extracted(base, {}, 0.2, 0, no_data={"m1": "unusable"})
    assert set(with_data.holdout_log_ids) == set(without.holdout_log_ids)
    assert set(with_data.fit_log_ids) == set(without.fit_log_ids)


def test_counts_partition_the_selected_logs():
    mismatched = [replace(make_data(f"m{k}"), cell_count=1) for k in range(4)]
    datas = [make_data(f"a{k}") for k in range(12)] + mismatched
    no_data = {"u1": "unusable", "l1": "load_error", "s1": "sim", "s2": "sim"}
    r = fit_extracted(datas, {}, 0.3, 0, no_data=no_data)
    held = set(r.holdout_log_ids)
    assert held & {d.log_id for d in mismatched}  # a mismatched log is held out
    assert {d.log_id for d in mismatched} - held  # and one is on the fit side
    assert not set(r.fit_log_ids) & set(r.holdout_scored_log_ids)
    total = (
        len(r.fit_log_ids) + len(r.holdout_scored_log_ids) + r.fit_logs_without_samples
        + r.holdout_logs_not_scored + r.sim_log_count + r.skipped_log_count
        + r.cell_count_mismatch_count)
    assert total == len(datas) + len(no_data)
    assert r.cell_count_mismatch_count == 4 and r.sim_log_count == 2 and r.skipped_log_count == 2


def _obs(log_id, cells, r_obs):
    return _LogObservations(log_id, cells, list(r_obs), [])


def _battery_doc(rows, pooled_vehicles):
    """A dict shaped like result_to_dict; rows are (cells, deployed, n, low_n)."""
    doc = battery_dict()
    doc["r_fits"] = [
        {"cell_count": c, "median_mohm_per_cell": d, "deployed_mohm_per_cell": d,
         "n_observations": n, "low_n": low} for c, d, n, low in rows]
    if pooled_vehicles is not None:
        doc["pooled_r_over_vehicles_mohm_per_cell"], doc["pooled_r_vehicles"] = pooled_vehicles
    return doc


def test_low_resistance_is_deployed():  # 6S packs at 3.2 mohm/cell, enough steps
    datas = [make_data(f"s{k:02d}", 6, 3.2) for k in range(16)]
    r = fit_extracted(datas, {}, 0.2, 0)
    six = next(f for f in r.r_fits if f.cell_count == 6)
    assert six.n_observations >= 200 and not six.low_n
    assert six.deployed_mohm_per_cell == six.median_mohm_per_cell
    assert six.deployed_mohm_per_cell == pytest.approx(3.2, rel=0.05)


def test_pooled_over_vehicles():
    obs = [_obs("a", 6, [2.0] * 1000), _obs("b", 4, [8.0] * 10), _obs("c", 4, [8.0] * 10)]
    rows, by_steps, n_steps, by_vehicles, n_vehicles = _r_table(obs, {"a": "vA", "b": "vB", "c": "vC"})
    assert (by_steps, by_vehicles, n_vehicles) == (2.0, 8.0, 3)


def test_few_steps_deploy_the_vehicle_pooled_median():
    obs = [_obs("a", 6, [2.0] * 1000), _obs("b", 4, [8.0] * 10), _obs("c", 4, [8.0] * 10)]
    rows, *_ = _r_table(obs, {"a": "vA", "b": "vB", "c": "vC"})
    four = next(f for f in rows if f.cell_count == 4)
    assert four.low_n and four.deployed_mohm_per_cell == 8.0
    six = next(f for f in rows if f.cell_count == 6)
    assert not six.low_n and six.deployed_mohm_per_cell == 2.0


def test_rust_comments_name_the_fallback():  # dict shaped like result_to_dict
    text = rust_constants_text(_battery_doc(rows=[(6, 3.2, 446961, False), (9, 5.9, 7, True)],
                                            pooled_vehicles=(5.99, 3)))
    assert "(6, 3.2000),  // n=446961\n" in text
    assert "(9, 5.9000),  // n=7, fewer than 200 step events -> pooled over vehicles" in text
    assert "FITTED_R_DEFAULT_MOHM_PER_CELL: f64 = 5.9900;  // pooled over 3 vehicles" in text
    assert "LOW-N" not in text


def test_rust_default_from_an_older_battery_file():
    doc = _battery_doc(rows=[(4, 9.0, 300, False)], pooled_vehicles=None)  # no new keys
    assert "f64 = 9.5000;  // pooled over step events, n=300" in rust_constants_text(doc)


OCV = OcvCurve([0.0, 1.0], [3.5, 4.2], 2)


def test_under_predicted_sag_is_positive():
    rows = holdout_by_cell_count([make_data("h1", 4, 12.0), make_data("h2", 4, 12.0)], OCV, lambda c: 9.0)
    (four,) = rows
    assert (four.cell_count, four.scored_logs, four.deployed_mohm_per_cell) == (4, 2, 9.0)
    assert four.median_signed_err_v_per_cell > 0


def test_over_predicted_sag_is_negative():
    (four,) = holdout_by_cell_count([make_data("h1", 4, 12.0)], OCV, lambda c: 15.0)
    assert four.median_signed_err_v_per_cell < 0


def test_one_row_per_cell_count_and_none_for_unscored():
    f1 = make_data("f1", 6)
    flat = replace(f1, remaining=np.full(f1.v.size, 0.5))
    rows = holdout_by_cell_count(
        [make_data("h1", 4), make_data("h2", 4), make_data("h3", 3, R_3S_MOHM), flat], OCV, lambda c: 9.0)
    assert [(r.cell_count, r.scored_logs) for r in rows] == [(3, 1), (4, 2)]


def test_fit_fills_the_check_per_cell_count_without_nan():
    r = fit_extracted([make_data(f"a{k}") for k in range(8)], {}, 0.2, 0)
    assert r.holdout_by_cell_count
    assert all(c.scored_logs > 0 for c in r.holdout_by_cell_count)
    doc = result_to_dict(r)
    json.dumps(doc, allow_nan=False)
    assert set(doc["holdout_by_cell_count"][0]) == {
        "cell_count", "scored_logs", "deployed_mohm_per_cell",
        "median_mae_v_per_cell", "median_signed_err_v_per_cell"}


def test_markdown_has_the_check_per_cell_count(tmp_path):
    r = fit_extracted([make_data(f"a{k}") for k in range(8)], {}, 0.2, 0)
    out = tmp_path / "s.md"
    write_markdown_summary(r, out)
    assert "Held-out check per cell count" in out.read_text()
