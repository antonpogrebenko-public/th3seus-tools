"""Tests for the held-out current-step check, which does not use PX4's state of charge."""

from __future__ import annotations

import dataclasses
import re
from pathlib import Path

import numpy as np
import pytest

from log_validation import battery_model, battery_output
from log_validation.battery_fit import (
    StepCheck,
    emit_rust_constants,
    fit_extracted,
    result_to_dict,
    step_check,
    write_markdown_summary,
)
from log_validation.tests.battery_fakes import CORPUS, R_4S_MOHM, make_data

REAL_IDS = [i for i in CORPUS if not i.startswith(("sim", "bad", "nobat", "flat"))]
NO_DATA = {
    "sim01": "sim", "sim02": "sim", "bad01": "load_error", "nobat01": "unusable",
    "flat01": "unusable",
}


def _corpus_result(**kwargs):
    datas = [make_data(i, 3 if i.startswith("c3") else 4,
                       11.0 if i.startswith("c3") else R_4S_MOHM) for i in REAL_IDS]
    return datas, fit_extracted(datas, {}, no_data=NO_DATA, **kwargs)


def _pack(log_id: str, cells: int, r_mohm: float, steps: int = 6):
    """A 5 Hz pack whose voltage drops exactly r_mohm per cell times the current."""
    n = steps * 4
    current = np.repeat(np.tile([1.0, 20.0], steps), 2)[:n]
    volts = cells * 4.0 - current * cells * r_mohm / 1000.0
    base = make_data(log_id, cells)
    return dataclasses.replace(
        base, t_s=np.arange(n) * 0.2, v=volts, i=current, remaining=None, discharged_mah=None
    )


class TestStepCheck:
    def test_known_resistance(self):
        datas, result = _corpus_result()
        sc = result.step_check
        assert isinstance(sc, StepCheck)
        assert sc.n_steps > 0
        assert sc.median_err_v_per_cell < 0.005

    def test_exact_r_gives_zero_error(self):
        sc = step_check([_pack("p1", 4, 9.0)], lambda cells: 9.0, {})
        assert sc.n_steps > 0
        assert sc.median_err_v_per_cell == pytest.approx(0.0, abs=1e-9)
        assert sc.p75_err_v_per_cell == pytest.approx(0.0, abs=1e-9)

    def test_wrong_r_shows_error(self):
        sc = step_check([_pack("p1", 4, 9.0)], lambda cells: 4.0, {})
        # dI = 19 A, 5 mOhm/cell off -> 0.095 V/cell
        assert sc.median_err_v_per_cell == pytest.approx(0.095, abs=1e-6)

    def test_constant_remaining_is_still_scored(self):
        pack = dataclasses.replace(_pack("p1", 4, 9.0), remaining=np.full(24, 0.5))
        sc = step_check([pack], lambda cells: 9.0, {})
        assert sc.n_steps > 0 and sc.n_vehicles == 1

    def test_no_steps_gives_none(self):
        flat = dataclasses.replace(_pack("p1", 4, 9.0), i=np.full(24, 3.0))
        sc = step_check([flat], lambda cells: 9.0, {})
        assert (sc.n_steps, sc.n_vehicles) == (0, 0)
        assert sc.median_err_v_per_cell is None and sc.p75_err_v_per_cell is None

    def test_slow_pairs_and_non_finite_are_skipped(self):
        pack = _pack("p1", 4, 9.0)
        slow = dataclasses.replace(pack, t_s=np.arange(24) * 3.0)
        assert step_check([slow], lambda cells: 9.0, {}).n_steps == 0
        v = pack.v.copy()
        v[1] = np.nan
        nan_pack = dataclasses.replace(pack, v=v)
        full = step_check([pack], lambda cells: 9.0, {}).n_steps
        assert step_check([nan_pack], lambda cells: 9.0, {}).n_steps == full - 1

    def test_vehicles_counted_once_and_unlisted_log_is_its_own(self):
        packs = [_pack(i, 4, 9.0) for i in ("p1", "p2", "p3")]
        sc = step_check(packs, lambda cells: 9.0, {"p1": "v1", "p2": "v1"})
        assert sc.n_vehicles == 2

    def test_does_not_mutate_inputs(self):
        pack = _pack("p1", 4, 9.0)
        before = (pack.t_s.copy(), pack.v.copy(), pack.i.copy())
        step_check([pack], lambda cells: 9.0, {})
        for a, b in zip(before, (pack.t_s, pack.v, pack.i)):
            assert np.array_equal(a, b)
        with pytest.raises(dataclasses.FrozenInstanceError):
            step_check([pack], lambda cells: 9.0, {}).n_steps = 1  # type: ignore[misc]


class TestResultWiring:
    def test_scored_ids_sorted_and_subset_of_holdout(self):
        _, result = _corpus_result()
        ids = result.holdout_scored_log_ids
        assert ids == tuple(sorted(ids)) and ids
        assert set(ids) <= set(result.holdout_log_ids)

    def test_log_with_no_remaining_counts_as_scored_by_steps(self):
        datas, base = _corpus_result()
        held = base.holdout_scored_log_ids
        target = held[0]
        changed = [dataclasses.replace(d, remaining=None) if d.log_id == target else d
                   for d in datas]
        result = fit_extracted(changed, {}, no_data=NO_DATA)
        assert target in result.holdout_scored_log_ids
        assert result.holdout_scored_count == base.holdout_scored_count - 1

    def test_json_and_markdown_show_the_check(self, tmp_path):
        _, result = _corpus_result()
        doc = result_to_dict(result)
        assert doc["step_check"]["n_steps"] == result.step_check.n_steps
        assert set(doc["step_check"]) == {
            "n_steps", "n_vehicles", "median_err_v_per_cell", "p75_err_v_per_cell"}
        assert doc["holdout_scored_log_ids"] == list(result.holdout_scored_log_ids)
        out = tmp_path / "s.md"
        write_markdown_summary(result, out)
        md = out.read_text()
        assert "Step check" in md and result.holdout_scored_log_ids[0] in md


EMIT_PATTERNS = [
    r"^const FITTED_R_TABLE: &\[\(u8, f64\)\] = &\[$",
    r"^    \(\d+, \d+\.\d{4}\),  // n=\d+",
    r"^const FITTED_R_DEFAULT_MOHM_PER_CELL: f64 = \d+\.\d{4};",
    r"^const OCV_TABLE: &\[\(f64, f64\)\] = &\[$",
    r"^    \(\d\.\d{4}, \d\.\d{4}\),$",
]


@pytest.mark.parametrize("pattern", EMIT_PATTERNS)
def test_emitted_rust_keeps_its_format(pattern):
    _, result = _corpus_result()
    lines = emit_rust_constants(result).splitlines()
    assert any(re.match(pattern, line) for line in lines), pattern


LONG_DASHES = ("—", "–")  # em dash, en dash


def _markdown(tmp_path, result) -> str:
    out = tmp_path / "s.md"
    write_markdown_summary(result, out)
    return out.read_text(encoding="utf-8")


def test_markdown_title_has_no_date_and_names_the_rust_format(tmp_path):
    _, result = _corpus_result()
    md = _markdown(tmp_path, result)
    assert md.splitlines()[0] == "# Battery Model Fit"
    assert "compiled into" not in md
    assert "the format `hitl-physics/src/battery.rs` uses" in " ".join(md.split())


def test_battery_outputs_and_sources_have_no_long_dashes(tmp_path):
    _, result = _corpus_result()
    texts = [_markdown(tmp_path, result), emit_rust_constants(result)]
    texts += [Path(m.__file__).read_text(encoding="utf-8") for m in (battery_output, battery_model)]
    assert [d for t in texts for d in LONG_DASHES if d in t] == []
