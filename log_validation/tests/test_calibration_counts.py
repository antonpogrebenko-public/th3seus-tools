"""Tests for the reason normalization and the per-pass counts."""

from __future__ import annotations

from log_validation.calibration_counts import battery_counts, entry_counts, normalize_reason
from log_validation.tests.calibration_inputs import battery_dict, noise_entries, response_entries


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
    assert (counts.analyzed, counts.skipped, counts.failed) == (3, 3, None)
    assert dict(counts.reasons)["simulation log excluded"] == 1
