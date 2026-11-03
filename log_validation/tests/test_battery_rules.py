"""Battery fit rules: which logs are left out and how they are counted."""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import replace

import numpy as np
import pytest

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


def test_the_median_decides_not_the_extremes():
    d = make_data("x")
    n = d.v.size
    high = np.full(n, 15.2)
    high[:8] = 18.5  # a few samples above 4.4 V per cell
    low = np.full(n, 11.0)
    low[:8] = 15.0  # median below 3.0 V per cell although some samples are in range
    noisy = np.full(n, 15.2)
    noisy[:3], noisy[3:6] = np.inf, np.nan
    assert cell_count_matches(replace(d, cell_count=4, v=high)) is True
    assert cell_count_matches(replace(d, cell_count=4, v=low)) is False
    assert cell_count_matches(replace(d, cell_count=4, v=noisy)) is True


def test_one_cell_pack_at_16_8_v_is_a_mismatch_and_left_out_of_the_fit():
    d = make_data("m1")
    mismatched = replace(d, cell_count=1, v=np.full(d.v.size, 16.8))
    assert cell_count_matches(mismatched) is False
    r = fit_extracted([make_data(f"a{k}") for k in range(8)] + [mismatched], {}, 0.2, 0)
    assert r.cell_count_mismatch_count == 1
    assert "m1" not in r.fit_log_ids and "m1" not in r.holdout_scored_log_ids


def test_matching_pack_is_kept_in_the_fit():
    datas = [make_data(f"a{k}") for k in range(8)]
    assert all(14.5 < float(np.median(d.v)) < 15.9 for d in datas)  # the fakes' 4S packs
    r = fit_extracted(datas, {}, 0.2, 0)
    assert r.cell_count_mismatch_count == 0
    kept = r.fit_log_ids[0]
    without = fit_extracted([d for d in datas if d.log_id != kept], {}, 0.2, 0, no_data={kept: "unusable"})
    steps = lambda res: next(f for f in res.r_fits if f.cell_count == 4).n_observations  # noqa: E731
    assert steps(r) > steps(without)


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
    text = out.read_text()
    assert "cell count does not match the pack voltage" in text
    assert ("| Left out: cell count does not match the pack voltage "
            "(median outside 3.0 to 4.4 V per cell) | 1 |") in text


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


def test_rust_comments_align_like_rustfmt():
    text = rust_constants_text(_battery_doc(
        rows=[(2, 13.4364, 4732, False), (6, 3.2146, 441198, False)], pooled_vehicles=(5.5699, 6380)))
    assert "    (2, 13.4364), // n=4732\n" in text
    assert "    (6, 3.2146),  // n=441198\n" in text
    assert "const FITTED_R_DEFAULT_MOHM_PER_CELL: f64 = 5.5699; // pooled over 6380 vehicles" in text


@pytest.mark.skipif(shutil.which("rustfmt") is None, reason="rustfmt is not installed")
def test_rust_constants_are_stable_under_rustfmt(tmp_path):
    doc = _battery_doc(rows=[(2, 13.4364, 4732, False), (6, 3.2146, 441198, False), (9, 5.5699, 7, True)],
                       pooled_vehicles=(5.5699, 6380))
    # a real-sized OCV curve: rustfmt joins a table that fits one line, the tool never emits one
    doc["ocv_curve"] = {"soc_knots": [k / 10 for k in range(11)],
                        "v_per_cell_knots": [3.3 + 0.09 * k for k in range(11)]}
    path = tmp_path / "battery_tables.rs"
    path.write_text(rust_constants_text(doc) + "\n")
    done = subprocess.run(["rustfmt", "--edition", "2021", "--check", str(path)],
                          capture_output=True, text=True)
    assert done.returncode == 0, done.stdout


def _long_row_doc():
    # the 6S line is 101 columns with one pad space (n has 78 digits), the 12S row is wider
    return _battery_doc(rows=[(12, 13.4364, 5, False), (6, 3.2146, 10 ** 77, False),
                              (7, 3.2146, 3, False)], pooled_vehicles=(5.5699, 6380))


def test_rust_comments_are_not_aligned_when_a_padded_row_would_pass_100_columns():
    # observed with rustfmt 1.9.0: one row that would pass 100 columns once padded drops the
    # alignment of the whole table to a single space
    text = rust_constants_text(_long_row_doc())
    assert "    (12, 13.4364), // n=5\n" in text
    assert "    (6, 3.2146), // n=" + "1" + "0" * 77 + "\n" in text
    assert "    (7, 3.2146), // n=3\n" in text


@pytest.mark.skipif(shutil.which("rustfmt") is None, reason="rustfmt is not installed")
def test_long_battery_rows_are_stable_under_rustfmt(tmp_path):
    doc = _long_row_doc()
    doc["ocv_curve"] = {"soc_knots": [k / 10 for k in range(11)],
                        "v_per_cell_knots": [3.3 + 0.09 * k for k in range(11)]}
    path = tmp_path / "battery_tables.rs"
    path.write_text(rust_constants_text(doc) + "\n")
    done = subprocess.run(["rustfmt", "--edition", "2021", "--check", str(path)],
                          capture_output=True, text=True)
    assert done.returncode == 0, done.stdout


def test_rust_comments_name_the_fallback():  # dict shaped like result_to_dict
    text = rust_constants_text(_battery_doc(rows=[(6, 3.2, 446961, False), (9, 5.9, 7, True)],
                                            pooled_vehicles=(5.99, 3)))
    assert "(6, 3.2000), // n=446961\n" in text
    assert "(9, 5.9000), // n=7, fewer than 200 step events -> pooled over vehicles" in text
    assert "FITTED_R_DEFAULT_MOHM_PER_CELL: f64 = 5.9900; // pooled over 3 vehicles" in text
    assert "LOW-N" not in text


def test_rust_comments_of_an_older_battery_file():
    reason = "implausible (median below the 4 mohm/cell floor)"
    doc = _battery_doc(rows=[(6, 3.27, 446961, True), (9, 5.9, 7, True)], pooled_vehicles=None)
    doc["r_fits"][0]["fallback_reason"] = reason
    lines = rust_constants_text(doc).splitlines()
    line6 = next(x for x in lines if x.startswith("    (6,"))
    assert f"n=446961, {reason} -> pooled median" in line6
    assert "fewer than 200" not in line6
    assert next(x for x in lines if x.startswith("    (9,")).endswith("// n=7 -> pooled median")


def test_rust_default_from_an_older_battery_file():
    doc = _battery_doc(rows=[(4, 9.0, 300, False)], pooled_vehicles=None)  # no new keys
    assert "f64 = 9.5000; // pooled over step events, n=300" in rust_constants_text(doc)


def test_pooled_over_vehicles_takes_each_vehicles_median_over_all_its_steps():
    obs = [_obs("a", 4, [2.0] * 100), _obs("b", 4, [8.0] * 10), _obs("c", 4, [4.0] * 10),
           _obs("d", 4, [9.0] * 10)]
    *_, by_vehicles, n_vehicles = _r_table(obs, {"a": "v1", "b": "v1", "c": "v2", "d": "v3"})
    assert (by_vehicles, n_vehicles) == (4.0, 3)  # v1's median is 2.0, not its log medians' 5.0


@pytest.mark.parametrize("n, low", [(199, True), (200, False)])
def test_the_200_step_boundary(n, low):
    obs = [_obs("a", 6, [2.0] * 1000), _obs("b", 4, [6.0] * n)]
    rows, _, _, by_vehicles, _ = _r_table(obs, {"a": "vA", "b": "vB"})
    four = next(f for f in rows if f.cell_count == 4)
    assert four.low_n is low
    assert four.deployed_mohm_per_cell == (by_vehicles if low else 6.0)


def test_unrowed_cell_count_is_scored_with_the_vehicle_pooled_default():
    datas = [make_data(f"a{k:02d}") for k in range(10)] + [make_data("five", 5)]
    for seed in range(60):
        r = fit_extracted(datas, {}, 0.3, seed)
        if "five" in r.holdout_log_ids and "five" in r.holdout_scored_log_ids:
            break
    else:
        pytest.fail("no seed holds out the 5S log")
    assert all(f.cell_count != 5 for f in r.r_fits)
    five = next(c for c in r.holdout_by_cell_count if c.cell_count == 5)
    assert five.deployed_mohm_per_cell == r.pooled_r_over_vehicles_mohm_per_cell


def test_headline_mae_of_an_unrowed_cell_count_uses_the_vehicle_pooled_default():
    datas = ([make_data(f"a{k}", 4, 9.0) for k in range(6)]
             + [make_data(f"b{k}", 4, 15.0) for k in range(3)] + [make_data("five", 5, 25.0)])
    vehicles = {**{f"a{k}": "vA" for k in range(6)}, **{f"b{k}": f"vB{k}" for k in range(3)},
                "five": "vF"}
    for seed in range(60):
        r = fit_extracted(datas, vehicles, 0.1, seed)
        if list(r.holdout_scored_log_ids) == ["five"]:
            break
    else:
        pytest.fail("no seed holds out only the 5S log")
    (row,) = r.holdout_by_cell_count
    assert row.cell_count == 5 and all(f.cell_count != 5 for f in r.r_fits)
    assert r.overall_r_median_mohm_per_cell != pytest.approx(r.pooled_r_over_vehicles_mohm_per_cell, abs=1.0)
    assert row.deployed_mohm_per_cell == r.pooled_r_over_vehicles_mohm_per_cell
    assert r.holdout_median_mae_v_per_cell == row.median_mae_v_per_cell


def test_caller_cannot_pass_the_fit_internal_mismatch_status():
    with pytest.raises(ValueError, match="cell_count_mismatch"):
        fit_extracted([make_data(f"a{k}") for k in range(8)], {}, 0.2, 0,
                      no_data={"x": "cell_count_mismatch"})


def test_mismatched_log_also_in_no_data_is_given_more_than_once():
    datas = [make_data(f"a{k}") for k in range(8)] + [replace(make_data("m1"), cell_count=1)]
    with pytest.raises(ValueError, match="given more than once"):
        fit_extracted(datas, {}, 0.2, 0, no_data={"m1": "unusable"})


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
    (only,) = r.holdout_by_cell_count  # all logs are 4S
    assert only.median_mae_v_per_cell == r.holdout_median_mae_v_per_cell
    assert only.scored_logs == r.holdout_scored_count
    doc = result_to_dict(r)
    json.dumps(doc, allow_nan=False)
    assert set(doc["holdout_by_cell_count"][0]) == {
        "cell_count", "scored_logs", "deployed_mohm_per_cell",
        "median_mae_v_per_cell", "median_signed_err_v_per_cell"}


def test_markdown_has_the_check_per_cell_count(tmp_path):
    r = fit_extracted([make_data(f"a{k}") for k in range(8)], {}, 0.2, 0)
    out = tmp_path / "s.md"
    write_markdown_summary(r, out)
    text = out.read_text()
    assert "Held-out check per cell count" in text
    (c,) = r.holdout_by_cell_count
    assert (f"| {c.cell_count}S | {c.scored_logs} | {c.deployed_mohm_per_cell:.2f} | "
            f"{c.median_mae_v_per_cell:.4f} | {c.median_signed_err_v_per_cell:.4f} |") in text
    assert (f"Pooled median over vehicles: {r.pooled_r_over_vehicles_mohm_per_cell:.2f} mohm/cell "
            f"({r.pooled_r_vehicles} vehicles)") in text
    assert ("Medians over the scored held-out logs of each cell count, next to the resistance "
            "deployed for it.") in " ".join(text.split())


def test_markdown_signed_error_sentence_and_single_blank_lines(tmp_path):
    r = fit_extracted([make_data(f"a{k}") for k in range(8)], {}, 0.2, 0)
    out = tmp_path / "s.md"
    write_markdown_summary(r, out)
    text = out.read_text()
    assert ("The signed error is predicted minus measured voltage per cell; positive means "
            "the model predicts less sag than the packs show, or that the open-circuit curve "
            "sits above these packs.") in " ".join(text.split())
    assert "\n\n\n" not in text
    assert "3.0 to 4.4 V per cell" in text


KNOWN_REASONS = ["low-n (fewer than 200 step events)",
                 "implausible (median below the 4 mohm/cell floor)"]


@pytest.mark.parametrize("bad", [
    "low-n\n fn injected() {}", "something else", "implausible\r\nfn x() {}", 7, ["low-n"]])
def test_older_file_fallback_reason_must_be_a_known_one(bad):
    doc = _battery_doc(rows=[(6, 3.27, 446961, True)], pooled_vehicles=None)
    doc["r_fits"][0]["fallback_reason"] = bad
    with pytest.raises(ValueError, match="fallback_reason"):
        rust_constants_text(doc)


@pytest.mark.parametrize("reason", KNOWN_REASONS)
def test_older_file_known_fallback_reasons_still_render(reason):
    doc = _battery_doc(rows=[(6, 3.27, 446961, True)], pooled_vehicles=None)
    doc["r_fits"][0]["fallback_reason"] = reason
    assert f"n=446961, {reason} -> pooled median" in rust_constants_text(doc)
