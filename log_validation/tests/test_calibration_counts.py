"""Tests for the reason normalization and the per-pass counts."""

from __future__ import annotations

from dataclasses import asdict, replace
from pathlib import Path

import numpy as np
import pytest

from log_validation.battery_fit import fit_extracted
from log_validation import response
from log_validation.batch_analyze import _died_result, analyze_log
from log_validation.battery_output import result_to_dict
from log_validation.calibration_counts import battery_counts, entry_counts, normalize_reason
from log_validation.calibration_ranges import log_id_lists
from log_validation.tests.battery_fakes import CORPUS, make_data
from log_validation.tests.calibration_inputs import battery_dict, noise_entries, response_entries
from log_validation.tests.response_fakes import quad_log


def test_numbers_and_parenthesized_details_give_one_key():
    a = normalize_reason("roll: low excitation (std 0.001 < 0.02)")
    b = normalize_reason("roll: low excitation (std 0.0034 < 0.02)")
    assert a == b == "roll: low excitation"


def test_numbers_become_n_and_space_collapses():
    assert normalize_reason("  rate  1.5e-3 Hz  below 200 ") == "rate N Hz below N"
    assert normalize_reason("esc[3] (a (nested) detail) lag") == "esc[N] lag"


def test_entry_counts_split_analyzed_skipped_failed():
    entries = noise_entries()
    ids = [e["log_id"] for e in entries if e["measurements"]]
    counts = entry_counts("noise", entries, ids)
    assert (counts.analyzed, counts.skipped, counts.failed) == (6, 1, 1)
    reasons = dict(counts.reasons)
    assert reasons["simulation log skipped"] == 1 and reasons["load failed: corrupt log"] == 1


def test_response_reasons_are_counted_by_normalized_key():
    entries = response_entries()
    counts = entry_counts("response", entries, [e["log_id"] for e in entries if e["measurements"]])
    assert dict(counts.reasons) == {"roll: low excitation": 2}
    assert counts.skipped == 2 and counts.failed == 0


def test_battery_counts_come_from_the_totals():
    counts = battery_counts(battery_dict(), ["a", "b", "c"])
    assert (counts.analyzed, counts.skipped, counts.failed) == (3, 4, None)
    assert dict(counts.reasons) == {"simulation log excluded": 1,
                                    "load error or no usable battery data": 2,
                                    "holdout log not scored": 1}


def test_battery_counts_of_an_older_json_without_the_new_totals():
    doc = {k: v for k, v in battery_dict().items()
           if k not in ("fit_logs_without_samples", "holdout_logs_not_scored")}
    counts = battery_counts(doc, ["a"])
    assert counts.skipped == 3 and "holdout log not scored" not in dict(counts.reasons)


# The battery pass over the fake corpus plus usable logs that give no sample: a constant
# current (no step) and no state of charge (no OCV sample, no MAE score).
REAL_IDS = [i for i in CORPUS if not i.startswith(("sim", "bad", "nobat", "flat"))]
NO_DATA = {"sim01": "sim", "sim02": "sim", "bad01": "load_error", "nobat01": "unusable",
           "flat01": "unusable"}
QUIET_IDS = [f"q{k:02d}" for k in range(8)]
WITHOUT_ROW = 2


def _quiet(log_id: str):
    base = make_data(log_id)
    return replace(base, i=np.full(base.i.shape, 30.0), remaining=None, discharged_mah=None)


def _fit_doc() -> dict:
    datas = [make_data(i, 3 if i.startswith("c3") else 4) for i in REAL_IDS]
    datas += [_quiet(i) for i in QUIET_IDS]
    datas.append(replace(make_data("m01", 4), cell_count=1))  # a 4S pack logged as 1S
    result = fit_extracted(datas, {}, no_data=NO_DATA, logs_without_manifest_row=WITHOUT_ROW)
    return result_to_dict(result)


def test_battery_counts_partition_the_logs_the_pass_read():
    doc = _fit_doc()
    counts = battery_counts(doc, log_id_lists([], doc, [])["battery"])
    read = len(REAL_IDS) + len(QUIET_IDS) + len(NO_DATA) + WITHOUT_ROW + 1
    assert counts.analyzed + counts.skipped == read
    assert sum(n for _, n in counts.reasons) == counts.skipped
    reasons = dict(counts.reasons)
    assert reasons["simulation log excluded"] == 2
    assert reasons["load error or no usable battery data"] == 3
    assert reasons["log without a manifest row, left out"] == WITHOUT_ROW
    assert reasons["cell count does not match the pack voltage"] == 1
    quiet = (reasons["fit log without a usable step or OCV sample"]
             + reasons["holdout log not scored"])
    assert quiet == len(QUIET_IDS)  # every quiet log, on either side, in exactly one row
    assert counts.analyzed == len(REAL_IDS)


def test_battery_json_has_the_unsampled_and_unscored_counts():
    doc = _fit_doc()
    fit_side = doc["fit_logs_without_samples"]
    holdout_side = doc["holdout_logs_not_scored"]
    assert fit_side >= 1 and holdout_side >= 1  # the seed puts quiet logs on both sides
    assert fit_side + holdout_side == len(QUIET_IDS)


def test_available_topic_and_field_lists_are_dropped():
    a = normalize_reason("gps.horizontal_noise_sigma: \"topic 'vehicle_gps_position' not in log. "
                         "Available topics: ['sensor_accel', 'sensor_gyro']\"")
    b = normalize_reason("gps.horizontal_noise_sigma: \"topic 'vehicle_gps_position' not in log. "
                         "Available topics: ['sensor_accel', 'sensor_combined', "
                         "'vehicle_air_data']\"")
    assert a == b == "gps.horizontal_noise_sigma: \"topic 'vehicle_gps_position' not in log.\""
    fields = normalize_reason("gyro.x: \"topic 'sensor_combined' missing field 'gyro_rad[2]'. "
                              "Available fields: ['gyro_rad[0]', 'gyro_rad[1]', 'timestamp']\"")
    assert fields == "gyro.x: \"topic 'sensor_combined' missing field 'gyro_rad[N]'.\""
    assert normalize_reason("Available fields: []") == ""


def test_worker_death_and_unavailable_count_as_failed():
    entries = [{"log_id": "a", "errors": ["worker process died"]},
               {"log_id": "b", "errors": ["parameters unavailable: AttributeError"]},
               {"log_id": "c", "errors": ["roll: low excitation (std 0.001 < 0.02)"]},
               {"log_id": "d", "errors": []}]
    counts = entry_counts("noise", entries, ["d"])
    assert (counts.analyzed, counts.skipped, counts.failed) == (1, 1, 2)
    assert dict(counts.reasons) == {"worker process died": 1,
                                    "parameters unavailable: AttributeError": 1,
                                    "roll: low excitation": 1}


def test_each_battery_total_has_its_reason_row_most_common_first():
    doc = {"sim_log_count": 1, "skipped_log_count": 2, "fit_logs_without_samples": 3,
           "holdout_logs_not_scored": 4, "logs_without_manifest_row": 5}
    counts = battery_counts(doc, [])
    assert counts.reasons == (("log without a manifest row, left out", 5),
                              ("holdout log not scored", 4),
                              ("fit log without a usable step or OCV sample", 3),
                              ("load error or no usable battery data", 2),
                              ("simulation log excluded", 1))
    assert (counts.analyzed, counts.skipped, counts.failed) == (0, 15, None)


DRV_FAILED_NOTE = ("gps.speed_accuracy_median_m_s: \"field 's_variance_m_s' not in topic "
                   "'vehicle_gps_position'. Available fields: ['drv_failed', 'eph']\"")


def _one(errors, contributing=()):
    counts = entry_counts("noise", [{"log_id": "a", "errors": errors}], list(contributing))
    return counts.analyzed, counts.skipped, counts.failed


@pytest.mark.parametrize("error", [
    DRV_FAILED_NOTE,
    "roll: fit failed (too few samples to fit)",
    "ESC RPM: no ESC fitted (esc[0]: fit failed (too few samples to fit))",
    "roll: fit failed: singular matrix",
    "parts: gps lookup unavailable: KeyError",
])
def test_a_note_that_mentions_a_failure_counts_as_skipped(error):
    assert _one([error]) == (0, 1, 0)


def test_failure_texts_the_passes_write_count_as_failed(monkeypatch):
    def unreadable(path):
        raise OSError(f"cannot open {path}")

    def broken(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(response, "detect_flight_segments", broken)
    results = [
        analyze_log(Path("n1.ulg"), loader=unreadable),
        response.analyze_response(quad_log(), "r1", "veh1", response.ResponseGates()),
        _died_result((Path("n2.ulg"), "veh2", unreadable, 30.0)),
    ]
    entries = [{**asdict(r), "log_id": f"log{i}"} for i, r in enumerate(results)]
    assert [e["errors"][0] for e in entries] == [
        "load failed: OSError",
        "flight segments failed: RuntimeError (effectiveness, hover, noise and motor lag "
        "not measured)",
        "worker process died"]
    counts = entry_counts("noise", entries, [])
    assert (counts.failed, counts.skipped, counts.analyzed) == (3, 0, 0)


@pytest.mark.parametrize("error", [
    "load failed: OSError", "parameters unavailable: KeyError",
    "hardware extraction failed: ValueError", "part identification failed: KeyError",
    "analysis failed: ValueError", "gyro rate parameters unavailable: KeyError",
    "flight segments failed: IndexError (effectiveness, hover, noise and motor lag "
    "not measured)", "battery scaling failed: ValueError",
    "motor lag failed: ValueError", "cell count failed: KeyError", "worker process died",
])
def test_every_failure_form_the_passes_write_counts_as_failed(error):
    assert _one([error, "gps.eph_median_m: no GPS topic"]) == (0, 0, 1)


def test_a_contributing_log_with_a_failure_text_is_analyzed():
    assert _one(["analysis failed: ValueError"], contributing=["a"]) == (1, 0, 0)
