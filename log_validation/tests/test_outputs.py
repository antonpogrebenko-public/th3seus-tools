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


def _identity(monkeypatch, user: str, host: str) -> None:
    monkeypatch.setattr(outputs.getpass, "getuser", lambda: user)
    monkeypatch.setattr(outputs.socket, "gethostname", lambda: host)


def test_forbidden_strings_include_home_and_extra(tmp_path):
    f = forbidden_strings(tmp_path)
    assert str(Path.home()) in f
    assert str(tmp_path.resolve()) in f


def test_forbidden_strings_include_the_user_and_host_name(tmp_path, monkeypatch):
    _identity(monkeypatch, "pilot-user", "bench-host")
    f = forbidden_strings()
    assert "pilot-user" in f and "bench-host" in f
    with pytest.raises(PrivateDataError):
        write_text(tmp_path / "o.md", "made on bench-host", f)
    assert not (tmp_path / "o.md").exists()


def test_strings_shorter_than_the_minimum_are_not_forbidden(monkeypatch):
    # A two-letter user or host name would refuse any output holding those letters.
    assert outputs.MIN_FORBIDDEN_LEN == 3
    _identity(monkeypatch, "ab", "xyz")
    f = forbidden_strings("/q")  # a path of two characters, given and resolved
    assert "ab" not in f and "/q" not in f
    assert "xyz" in f


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


def _chunks(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Chunk 1 holds a01..a04 of one vehicle, chunk 2 a05, a06 and a log without a row."""
    p1, p2 = tmp_path / "1.json", tmp_path / "2.json"
    p1.write_text(json.dumps([{"log_id": f"a0{i}"} for i in range(1, 5)]))
    p2.write_text(json.dumps([{"log_id": "a05"}, {"log_id": "a06"}, {"log_id": "z99"}]))
    manifest = tmp_path / "m.csv"
    manifest.write_text("log_id,vehicle_id\n" + "".join(f"a0{i},v1\n" for i in range(1, 7)))
    return p1, p2, manifest


def test_merge_with_a_manifest_recaps_over_all_chunks(tmp_path, capsys):
    p1, p2, manifest = _chunks(tmp_path)
    out = tmp_path / "merged.json"
    code = outputs.main(["merge", "-o", str(out), "--manifest", str(manifest),
                         "--max-per-vehicle", "3", str(p1), str(p2)])
    assert code == 0
    ids = [r["log_id"] for r in json.loads(out.read_text())]
    assert ids == ["a01", "a02", "a03", "z99"]
    assert "dropped 3" in capsys.readouterr().err


def test_merge_cap_defaults_to_3_with_a_manifest(tmp_path):
    p1, p2, manifest = _chunks(tmp_path)
    out = tmp_path / "merged.json"
    assert outputs.main(["merge", "-o", str(out), "--manifest", str(manifest), str(p1), str(p2)]) == 0
    assert [r["log_id"] for r in json.loads(out.read_text())] == ["a01", "a02", "a03", "z99"]


def test_merge_cap_needs_a_manifest(tmp_path):
    p1, p2, _ = _chunks(tmp_path)
    with pytest.raises(SystemExit) as stop:
        outputs.main(["merge", "-o", str(tmp_path / "o.json"), "--max-per-vehicle", "2",
                      str(p1), str(p2)])
    assert stop.value.code == 2


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


def _merge_args(tmp_path: Path, *parts: Path) -> list[str]:
    return ["merge", "-o", str(tmp_path / "out" / "merged.json"), *map(str, parts)]


@pytest.mark.parametrize("content, message", [
    ([{"log_id": "a"}, {"log_id": "a"}], "more than one part: a"),
    ({"log_id": "a"}, "expected a JSON list"),
    ([{"vehicle_id": "v1"}], "log_id"),
])
def test_merge_command_errors_exit_2(tmp_path, capsys, content, message):
    part = tmp_path / "1.json"
    part.write_text(json.dumps(content))
    assert outputs.main(_merge_args(tmp_path, part)) == 2
    err = capsys.readouterr().err
    assert err.startswith("Error: ") and message in err
    assert not (tmp_path / "out").exists()


def test_merge_command_missing_part_exits_2(tmp_path, capsys):
    assert outputs.main(_merge_args(tmp_path, tmp_path / "absent.json")) == 2
    assert capsys.readouterr().err.startswith("Error: ")


def test_merge_command_refuses_private_data(tmp_path, capsys):
    part = tmp_path / "1.json"
    part.write_text(json.dumps([{"log_id": "a", "note": f"seen in {Path.home()}"}]))
    assert outputs.main(_merge_args(tmp_path, part)) == 2
    assert "private data" in capsys.readouterr().err
    assert not (tmp_path / "out" / "merged.json").exists()
