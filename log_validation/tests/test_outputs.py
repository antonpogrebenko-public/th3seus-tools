"""Guarded, deterministic output writing and per-log merging."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from log_validation import outputs
from log_validation.outputs import (
    PrivateDataError,
    forbidden_strings,
    merge_per_log,
    write_json,
    write_text,
)


def test_write_json_refuses_forbidden(tmp_path):
    target = tmp_path / "o.json"
    with pytest.raises(PrivateDataError):
        write_json(target, {"p": "/secret/logs/x.ulg"}, ["/secret/logs"])
    assert not target.exists()


def test_write_text_refuses_forbidden(tmp_path):
    target = tmp_path / "o.md"
    with pytest.raises(PrivateDataError):
        write_text(target, "see /secret/logs", ["/secret/logs"])
    assert not target.exists()
    write_text(target, "fine", ["/secret/logs"])
    assert target.read_text() == "fine"


def test_write_json_is_deterministic(tmp_path):
    a = {"b": 1, "a": {"y": 2, "x": 3}}
    b = {"a": {"x": 3, "y": 2}, "b": 1}
    write_json(tmp_path / "a.json", a, [])
    write_json(tmp_path / "b.json", b, [])
    assert (tmp_path / "a.json").read_bytes() == (tmp_path / "b.json").read_bytes()
    assert list(json.loads((tmp_path / "a.json").read_text())) == ["a", "b"]


def test_nan_is_written_as_null(tmp_path):
    write_json(tmp_path / "o.json", {"v": float("nan"), "w": float("inf"), "l": [-float("inf")]}, [])
    assert json.loads((tmp_path / "o.json").read_text()) == {"v": None, "w": None, "l": [None]}


def test_numpy_values_are_converted(tmp_path):
    obj = {"s": np.float64(1.5), "i": np.int64(2), "a": np.array([1.0, np.nan])}
    write_json(tmp_path / "o.json", obj, [])
    assert json.loads((tmp_path / "o.json").read_text()) == {"s": 1.5, "i": 2.0, "a": [1.0, None]}


def test_merge_sorts_and_rejects_repeats(tmp_path):
    p1, p2 = tmp_path / "1.json", tmp_path / "2.json"
    p1.write_text(json.dumps([{"log_id": "b"}]))
    p2.write_text(json.dumps([{"log_id": "a"}]))
    assert merge_per_log([p1, p2]) == [{"log_id": "a"}, {"log_id": "b"}]
    p2.write_text(json.dumps([{"log_id": "b"}]))
    with pytest.raises(ValueError, match="b"):
        merge_per_log([p1, p2])


def test_forbidden_strings_include_home_and_extra(tmp_path):
    f = forbidden_strings(tmp_path)
    assert str(Path.home()) in f
    assert str(tmp_path.resolve()) in f
    assert all(len(s) >= 3 for s in forbidden_strings("ab", tmp_path))


def test_bare_relative_name_is_forbidden_only_resolved(tmp_path, monkeypatch):
    # "obs" alone would refuse any output containing those letters
    # (e.g. "n_total_observations"); its absolute path is still forbidden.
    monkeypatch.chdir(tmp_path)
    f = forbidden_strings("obs")
    assert "obs" not in f
    assert str((tmp_path / "obs").resolve()) in f


def test_relative_path_with_a_separator_is_forbidden_as_given():
    assert "data/downloaded" in forbidden_strings("data/downloaded")


def test_merge_command(tmp_path):
    p1, p2 = tmp_path / "1.json", tmp_path / "2.json"
    p1.write_text(json.dumps([{"log_id": "b"}]))
    p2.write_text(json.dumps([{"log_id": "a"}]))
    out = tmp_path / "out" / "merged.json"
    assert outputs.main(["merge", "-o", str(out), str(p1), str(p2)]) == 0
    assert json.loads(out.read_text()) == [{"log_id": "a"}, {"log_id": "b"}]


def test_write_json_refuses_non_ascii_forbidden(tmp_path):
    target = tmp_path / "o.json"
    with pytest.raises(PrivateDataError):
        write_json(target, {"p": "/home/jos\u00e9/logs/x.ulg"}, ["/home/jos\u00e9"])
    assert not target.exists()


def test_write_json_refuses_backslash_forbidden(tmp_path):
    target = tmp_path / "o.json"
    with pytest.raises(PrivateDataError):
        write_json(target, {"p": "C:\\Users\\bob\\x.ulg"}, ["C:\\Users\\bob"])
    assert not target.exists()


def test_write_json_is_utf8(tmp_path):
    write_json(tmp_path / "o.json", {"k": "caf\u00e9"}, [])
    assert "caf\u00e9".encode("utf-8") in (tmp_path / "o.json").read_bytes()
