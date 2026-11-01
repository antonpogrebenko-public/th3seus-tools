"""Guarded, deterministic output writing and per-log merging."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import numpy as np
import pytest

from log_validation import outputs
from log_validation.outputs import (
    PrivateDataError,
    forbidden_strings,
    json_text,
    merge_per_log,
    read_json,
    write_json,
    write_json_or_gz,
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


def test_input_forbidden_holds_the_input_folders_and_extras(tmp_path):
    folder = tmp_path / "in"
    folder.mkdir()
    got = outputs.input_forbidden([folder / "a.json"], ["acct-123"])
    assert str(folder.absolute()) in got and str(folder.resolve()) in got
    assert "acct-123" in got and str(Path.home()) in got


def test_input_forbidden_refuses_a_short_extra(tmp_path):
    with pytest.raises(ValueError):
        outputs.input_forbidden([tmp_path / "a.json"], ["ab"])


def test_write_staged_copies_into_a_new_folder(tmp_path):
    out = tmp_path / "out"
    outputs.write_staged(out, lambda stage: write_text(stage / "d" / "a.txt", "x\n", []))
    assert (out / "d" / "a.txt").read_text() == "x\n"


def test_write_staged_leaves_the_folder_alone_when_a_write_fails(tmp_path):
    out = tmp_path / "out"

    def stage_files(stage):
        write_text(stage / "a.txt", "fine\n", ["secret"])
        write_text(stage / "b.txt", "secret\n", ["secret"])

    with pytest.raises(PrivateDataError):
        outputs.write_staged(out, stage_files)
    assert not out.exists()


def test_write_staged_leaves_the_folder_alone_when_the_copy_fails(tmp_path, monkeypatch):
    def copy_one_then_fail(src, dst, *args, **kwargs):
        first = sorted(Path(src).iterdir())[0]
        Path(dst).mkdir(parents=True, exist_ok=True)
        (Path(dst) / first.name).write_bytes(first.read_bytes())
        raise OSError("disk full")

    def stage_files(stage):
        write_text(stage / "a.txt", "x\n", [])
        write_text(stage / "b.txt", "y\n", [])

    monkeypatch.setattr(outputs.shutil, "copytree", copy_one_then_fail)
    with pytest.raises(OSError):
        outputs.write_staged(tmp_path / "out", stage_files)
    assert list(tmp_path.iterdir()) == []  # no partial folder, no copy left beside it


def test_write_staged_fills_an_existing_empty_folder(tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    outputs.write_staged(out, lambda stage: write_text(stage / "a.txt", "x\n", []))
    assert (out / "a.txt").read_text() == "x\n" and list(tmp_path.iterdir()) == [out]


def test_write_staged_refuses_a_non_empty_folder(tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    (out / "old.txt").write_text("keep")
    with pytest.raises(ValueError):
        outputs.write_staged(out, lambda stage: write_text(stage / "a.txt", "x", []))
    assert [p.name for p in out.iterdir()] == ["old.txt"]


BIG = [{"a": i} for i in range(100)]


def test_large_json_is_written_as_deterministic_gzip(tmp_path):
    p = write_json_or_gz(tmp_path / "r.json", BIG, (), over_bytes=100)
    data = p.read_bytes()
    assert p.name == "r.json.gz" and not (tmp_path / "r.json").exists()
    assert data[4:8] == b"\x00\x00\x00\x00"
    assert gzip.decompress(data).decode() == json_text(BIG)
    again = write_json_or_gz(tmp_path / "s.json", BIG, (), over_bytes=100)
    assert again.read_bytes() == data


def test_gzip_header_has_no_platform_byte(tmp_path):
    data = write_json_or_gz(tmp_path / "r.json", BIG, (), over_bytes=100).read_bytes()
    assert data[9] == 255 and data[4:8] == b"\x00\x00\x00\x00"


def test_small_json_stays_plain(tmp_path):
    p = write_json_or_gz(tmp_path / "r.json", BIG, ())
    assert p.name == "r.json" and p.read_text(encoding="utf-8") == json_text(BIG)


def test_size_at_the_limit_stays_plain(tmp_path):
    size = len(json_text(BIG).encode("utf-8"))
    assert write_json_or_gz(tmp_path / "a.json", BIG, (), over_bytes=size).name == "a.json"
    assert write_json_or_gz(tmp_path / "b.json", BIG, (), over_bytes=size - 1).name == "b.json.gz"


def test_gzip_refuses_private_data_before_writing(tmp_path):
    with pytest.raises(PrivateDataError):
        write_json_or_gz(tmp_path / "r.json", [{"p": "/secret/x"}] * 50, ["/secret"],
                         over_bytes=10)
    assert list(tmp_path.iterdir()) == []


def test_read_json_reads_both(tmp_path):
    plain = write_json_or_gz(tmp_path / "a.json", BIG, ())
    packed = write_json_or_gz(tmp_path / "b.json", BIG, (), over_bytes=10)
    assert read_json(plain) == read_json(packed) == BIG


@pytest.mark.parametrize("make", [
    lambda: gzip.compress(b'[{"a": 1}]' * 50)[:-20],
    lambda: b"this is not gzip",
    lambda: gzip.compress(b"\xff\xfe not utf8"),
    lambda: gzip.compress(b"{not json"),
])
def test_read_json_maps_corrupt_gzip_to_value_error(tmp_path, make):
    path = tmp_path / "x.json.gz"
    path.write_bytes(make())
    with pytest.raises(ValueError):
        read_json(path)
