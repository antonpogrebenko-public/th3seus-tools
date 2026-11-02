"""Battery fit rules: which logs are left out and how they are counted."""

from __future__ import annotations

from dataclasses import replace

import numpy as np
import pytest

from log_validation.battery_fit import cell_count_matches, fit_extracted
from log_validation.battery_output import result_to_dict, write_markdown_summary
from log_validation.tests.battery_fakes import make_data


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
