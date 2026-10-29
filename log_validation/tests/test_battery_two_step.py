"""Tests for the two-step battery flow: extract to folders, fit from them, and the CLI."""

from __future__ import annotations

import json
import pickle
import subprocess
import sys
from pathlib import Path

import pytest

from log_validation import battery_extract
from log_validation.battery_fit import (
    analyze_logs,
    fit_extracted,
    extract_dir,
    fit_obs_dir,
    main,
    result_to_dict,
    save_extracted,
)
from log_validation.tests.battery_fakes import (
    CORPUS,
    load_by_stem,
    make_data,
    make_log_dir,
    make_manifest,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
# Six vehicles over the corpus; a09 has no manifest row.
ROWS_WITHOUT_A09 = {i: f"v{k % 6}" for k, i in enumerate(CORPUS) if i != "a09"}


class TestFitObsDir:
    def test_manifest_one_shot_equals_extract_then_fit(self, tmp_path):
        rows = {i: f"v{k % 6}" for k, i in enumerate(CORPUS) if i != "a09"}
        manifest = make_manifest(tmp_path, rows)
        log_dir = make_log_dir(tmp_path, CORPUS)
        one_shot = analyze_logs(log_dir, manifest_path=manifest, loader=load_by_stem)
        obs = tmp_path / "obs"
        extract_dir(log_dir, obs, loader=load_by_stem)  # every log, no manifest
        assert result_to_dict(fit_obs_dir(obs, manifest)) == result_to_dict(one_shot)
        assert one_shot.logs_without_manifest_row == 1
        assert "a09" not in one_shot.fit_log_ids + one_shot.holdout_log_ids
        fit_vehicles = {rows[i] for i in one_shot.fit_log_ids}
        assert not fit_vehicles & {rows[i] for i in one_shot.holdout_log_ids}

    def test_cap_applies_to_the_fit(self, tmp_path):
        ids = [f"a{k:02d}" for k in range(8)] + ["c3-01", "c3-02"]
        manifest = make_manifest(tmp_path, {**{i: "v1" for i in ids[:8]}, "c3-01": "v2", "c3-02": "v3"})
        obs = tmp_path / "obs"
        extract_dir(make_log_dir(tmp_path, ids), obs, loader=load_by_stem)  # no cap here
        result = fit_obs_dir(obs, manifest, max_per_vehicle=3)
        counted = result.fit_log_ids + result.holdout_log_ids
        assert sorted(i for i in counted if i.startswith("a")) == ["a00", "a01", "a02"]
        assert result.real_log_count == 5

    def test_log_id_with_two_files_is_refused(self, tmp_path):
        obs = tmp_path / "obs"
        save_extracted(make_data("a01"), obs)
        (obs / "a01.sim").touch()
        with pytest.raises(ValueError, match="a01"):
            fit_obs_dir(obs)

    def test_extraction_with_a_manifest_keeps_the_count_of_logs_without_a_row(self, tmp_path):
        manifest = make_manifest(tmp_path, ROWS_WITHOUT_A09)
        log_dir = make_log_dir(tmp_path, CORPUS)
        one_shot = analyze_logs(log_dir, manifest_path=manifest, loader=load_by_stem)
        obs = tmp_path / "obs"
        extract_dir(log_dir, obs, manifest, loader=load_by_stem)
        assert (obs / "a09.no_manifest_row").exists()
        two_step = fit_obs_dir(obs, manifest)
        assert result_to_dict(two_step) == result_to_dict(one_shot)
        assert two_step.logs_without_manifest_row == 1
        without_manifest = fit_obs_dir(obs)  # the marker still counts, and only once
        assert without_manifest.logs_without_manifest_row == 1
        assert "a09" not in without_manifest.fit_log_ids + without_manifest.holdout_log_ids

    def test_two_extraction_folders_merge(self, tmp_path, monkeypatch):
        manifest = make_manifest(tmp_path, ROWS_WITHOUT_A09)
        one_shot = analyze_logs(make_log_dir(tmp_path, CORPUS), manifest_path=manifest,
                                loader=load_by_stem)
        obs = [tmp_path / "obs0", tmp_path / "obs1"]
        for k, stems in enumerate((CORPUS[0::2], CORPUS[1::2])):
            batch = tmp_path / f"batch{k}"
            batch.mkdir()
            for s in stems:
                (batch / f"{s}.ulg").write_bytes(b"")
            extract_dir(batch, obs[k], manifest, loader=load_by_stem)
        assert result_to_dict(fit_obs_dir(obs, manifest)) == result_to_dict(one_shot)
        out = tmp_path / "out.json"
        monkeypatch.setattr(battery_extract, "ULog", lambda p, message_name_filter_list=None: 0)
        args = ["--obs-dir", str(obs[0]), str(obs[1]), "--manifest", str(manifest)]
        assert main([*args, "--out-json", str(out), "--out-md", str(tmp_path / "o.md")]) in (0, 1)
        assert json.loads(out.read_text())["logs_without_manifest_row"] == 1
        with pytest.raises(ValueError, match="more than one"):
            fit_obs_dir([obs[0], obs[0]], manifest)


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------


class TestCli:
    def test_extract_then_fit_matches_log_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            battery_extract, "ULog", lambda path, message_name_filter_list=None: load_by_stem(Path(path))
        )
        log_dir = make_log_dir(tmp_path, CORPUS)
        obs, out = tmp_path / "obs", tmp_path / "out"
        assert main(["--log-dir", str(log_dir), "--extract-to", str(obs)]) == 0
        assert len(list(obs.glob("*.npz"))) == 16
        main(["--obs-dir", str(obs), "--out-json", str(out / "obs.json"),
              "--out-md", str(out / "obs.md")])
        main(["--log-dir", str(log_dir), "--out-json", str(out / "dir.json"),
              "--out-md", str(out / "dir.md")])
        text = (out / "dir.json").read_text()
        assert (out / "obs.json").read_text() == text
        assert (out / "obs.md").read_text() == (out / "dir.md").read_text()
        assert str(tmp_path) not in text and str(tmp_path) not in (out / "dir.md").read_text()
        doc = json.loads(text)
        assert {"per_log_r", "fit_log_ids", "holdout_log_ids", "logs_without_manifest_row"} <= set(doc)
        assert {"p10_mohm_per_cell", "p90_mohm_per_cell", "n_vehicles"} <= set(doc["r_fits"][0])
        md = (out / "dir.md").read_text()
        assert "p10" in md and doc["fit_log_ids"][0] in md and doc["holdout_log_ids"][0] in md

    def test_relative_obs_dir_writes_its_json(self, tmp_path, monkeypatch):
        extract_dir(make_log_dir(tmp_path, CORPUS), tmp_path / "obs", loader=load_by_stem)
        monkeypatch.chdir(tmp_path)
        code = main(["--obs-dir", "obs", "--out-json", "out/fit.json", "--out-md", "out/fit.md"])
        assert code in (0, 1)  # 2 would be an error such as a refused output
        assert "n_total_observations" in (tmp_path / "out" / "fit.json").read_text()


def test_outputs_do_not_import_the_fit(tmp_path):
    result = fit_extracted([make_data(f"a{k}") for k in range(6)], {}, 0.2, 0)
    pickled = tmp_path / "result.pkl"
    pickled.write_bytes(pickle.dumps(result))
    script = (
        "import pickle, sys\n"
        "from log_validation import battery_output\n"
        f"r = pickle.loads(open({str(pickled)!r}, 'rb').read())\n"
        f"battery_output.write_markdown_summary(r, {str(tmp_path / 'r.md')!r})\n"
        "print('log_validation.battery_fit' in sys.modules)\n"
    )
    done = subprocess.run([sys.executable, "-c", script], cwd=REPO_ROOT,
                          capture_output=True, text=True, check=True)
    assert done.stdout.strip() == "False"


@pytest.mark.parametrize("path", ["one_shot", "two_step"])
def test_negative_cap_is_refused_on_both_paths(tmp_path, path):
    manifest = make_manifest(tmp_path, {i: "v1" for i in CORPUS})
    log_dir = make_log_dir(tmp_path, CORPUS)
    obs = tmp_path / "obs"
    if path == "two_step":
        extract_dir(log_dir, obs, loader=load_by_stem)
    with pytest.raises(ValueError, match="max_per_vehicle"):
        if path == "one_shot":
            analyze_logs(log_dir, manifest_path=manifest, max_per_vehicle=-1, loader=load_by_stem)
        else:
            fit_obs_dir(obs, manifest, max_per_vehicle=-1)


def _stop_at_stop01(path: Path):
    """A loader that interrupts the run at the log 'stop01', as Ctrl-C or a kill would."""
    if path.stem == "stop01":
        raise KeyboardInterrupt
    return load_by_stem(path)


class TestInterruptedExtraction:
    def test_interrupted_extraction_is_refused(self, tmp_path):
        obs = tmp_path / "obs"
        with pytest.raises(KeyboardInterrupt):
            extract_dir(make_log_dir(tmp_path, CORPUS + ["stop01"]), obs, loader=_stop_at_stop01)
        assert (obs / "a00.npz").exists()  # finished logs are on disk
        with pytest.raises(ValueError, match="interrupted"):
            fit_obs_dir(obs)

    def test_interrupted_rerun_into_a_finished_folder_is_refused(self, tmp_path):
        log_dir = make_log_dir(tmp_path, CORPUS + ["stop01"])
        obs = tmp_path / "obs"
        extract_dir(log_dir, obs, loader=load_by_stem)  # stop01 loads as a 4S log here
        with pytest.raises(KeyboardInterrupt):
            extract_dir(log_dir, obs, loader=_stop_at_stop01)
        with pytest.raises(ValueError, match="interrupted"):
            fit_obs_dir(obs)

    def test_interrupted_batch_merged_into_one_folder_is_refused(self, tmp_path):
        obs = tmp_path / "obs"
        for k, stems in enumerate((CORPUS, ["aa99", "stop01", "zz01"])):
            batch = tmp_path / f"batch{k}"
            batch.mkdir()
            for s in stems:
                (batch / f"{s}.ulg").write_bytes(b"")
            if k == 0:
                extract_dir(batch, obs, loader=load_by_stem)
            else:
                with pytest.raises(KeyboardInterrupt):
                    extract_dir(batch, obs, loader=_stop_at_stop01)
        assert (obs / "aa99.npz").exists()  # finished before the stop, in the merged folder
        with pytest.raises(ValueError, match="interrupted"):
            fit_obs_dir(obs)

    def test_finished_batches_merged_into_one_folder_fit(self, tmp_path):
        obs = tmp_path / "obs"
        for k, stems in enumerate((CORPUS[0::2], CORPUS[1::2])):
            batch = tmp_path / f"batch{k}"
            batch.mkdir()
            for s in stems:
                (batch / f"{s}.ulg").write_bytes(b"")
            extract_dir(batch, obs, loader=load_by_stem)
        one_shot = analyze_logs(make_log_dir(tmp_path, CORPUS), loader=load_by_stem)
        assert result_to_dict(fit_obs_dir(obs)) == result_to_dict(one_shot)
