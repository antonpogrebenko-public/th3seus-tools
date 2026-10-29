"""Tests for the battery extract step (battery_extract.py)."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest

from log_validation import battery_extract
from log_validation.battery_fit import extract_dir, extract_log, load_extracted, save_extracted
from log_validation.tests.battery_fakes import (
    FAKE_SAMPLES,
    R_4S_MOHM,
    battery_status,
    fake_from,
    file_names,
    load_by_stem,
    make_data,
    make_log_dir,
    make_manifest,
)

# ---------------------------------------------------------------------------
# Extract step
# ---------------------------------------------------------------------------


class TestExtract:
    def test_extract_round_trip(self, tmp_path):
        data = make_data("a01")
        path = save_extracted(data, tmp_path)
        loaded = load_extracted(path)
        assert path == tmp_path / "a01.npz"
        assert (loaded.log_id, loaded.cell_count) == ("a01", 4)
        for name in ("t_s", "v", "i", "remaining"):
            np.testing.assert_array_equal(getattr(loaded, name), getattr(data, name))
        assert loaded.discharged_mah is None

    def test_round_trip_keeps_discharged_without_remaining(self, tmp_path):
        disch = np.linspace(0.0, 900.0, FAKE_SAMPLES)
        data = replace(make_data("a01"), remaining=None, discharged_mah=disch)
        loaded = load_extracted(save_extracted(data, tmp_path))
        assert loaded.remaining is None
        np.testing.assert_array_equal(loaded.discharged_mah, disch)

    @pytest.mark.parametrize(
        "stem, status",
        [("a01", "ok"), ("sim01", "sim"), ("bad01", "load_error"),
         ("nobat01", "unusable"), ("flat01", "unusable")],
    )
    def test_extract_log_status(self, stem, status):
        got, data = extract_log(Path(f"{stem}.ulg"), loader=load_by_stem)
        assert got == status
        assert (data is not None) == (status == "ok")

    def test_cell_count_out_of_range_is_unusable(self):
        battery = {**battery_status(4, R_4S_MOHM, 1), "cell_count": np.full(FAKE_SAMPLES, 15)}
        assert extract_log(Path("x.ulg"), loader=fake_from(battery)) == ("unusable", None)

    def test_default_loader_reads_only_the_battery_topic(self, monkeypatch):
        calls = []

        def fake_ulog(path, message_name_filter_list=None):
            calls.append(message_name_filter_list)
            return load_by_stem(Path(path))

        monkeypatch.setattr(battery_extract, "ULog", fake_ulog)
        status, _ = extract_log(Path("a01.ulg"))
        assert status == "ok" and calls == [["battery_status"]]

    def test_cap_applies_to_extraction(self, tmp_path):
        ids = [f"a{k:02d}" for k in range(8)]
        manifest = make_manifest(tmp_path, {i: "v1" for i in ids})
        out = tmp_path / "obs"
        counts = extract_dir(make_log_dir(tmp_path, ids), out, manifest, max_per_vehicle=3,
                             loader=load_by_stem)
        assert file_names(out) == ["a00.npz", "a01.npz", "a02.npz"]
        assert counts["ok"] == 3 and counts["capped_out"] == 5

    def test_every_selected_log_leaves_one_file(self, tmp_path):
        out = tmp_path / "obs"
        counts = extract_dir(make_log_dir(tmp_path, ["a01", "sim01", "bad01", "flat01"]), out,
                             loader=load_by_stem)
        assert file_names(out) == ["a01.npz", "bad01.load_error", "flat01.unusable", "sim01.sim"]
        assert (counts["ok"], counts["sim"], counts["unusable"], counts["load_error"]) == (1, 1, 1, 1)

    def test_jobs_do_not_change_the_extraction(self, tmp_path):
        log_dir = make_log_dir(tmp_path, ["a01", "sim01", "c3-01", "bad01"])
        one, two = tmp_path / "one", tmp_path / "two"
        assert extract_dir(log_dir, one, jobs=1, loader=load_by_stem) == extract_dir(
            log_dir, two, jobs=2, loader=load_by_stem
        )
        assert file_names(one) == file_names(two)
        for p in one.glob("*.npz"):
            np.testing.assert_array_equal(load_extracted(p).v, load_extracted(two / p.name).v)

    def test_dead_worker_does_not_stop_the_extraction(self, tmp_path):
        out = tmp_path / "obs"
        counts = extract_dir(make_log_dir(tmp_path, ["a01", "die01", "c3-01"]), out, jobs=2,
                             loader=load_by_stem)
        assert (counts["ok"], counts["worker_died"]) == (2, 1)
        assert file_names(out) == ["a01.npz", "c3-01.npz", "die01.worker_died"]

    def test_reextraction_replaces_a_stale_status(self, tmp_path):
        out = tmp_path / "obs"
        out.mkdir()
        (out / "a01.unusable").touch()
        extract_dir(make_log_dir(tmp_path, ["a01"]), out, loader=load_by_stem)
        assert file_names(out) == ["a01.npz"]

    def test_folder_readers_are_public(self, tmp_path):
        from log_validation.battery_extract import load_named, read_statuses

        out = tmp_path / "obs"
        extract_dir(make_log_dir(tmp_path, ["a01", "sim01"]), out, loader=load_by_stem)
        assert read_statuses(out) == {"a01": "ok", "sim01": "sim"}
        assert load_named(out, "a01").log_id == "a01"
