"""Tests for run_per_log: ordering, serial/parallel parity and worker-death containment."""

from __future__ import annotations

import os
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pytest

import log_validation.parallel as parallel
from log_validation.parallel import run_per_log

DIED = -1
DEATH_DELAY_S = 0.5


def square_or_die(n: int) -> int:
    """Top-level so a spawned worker can import it."""
    if n == 3:
        os._exit(1)
    return n * n


def square(n: int) -> int:
    return n * n


def raise_for_two(n: int) -> int:
    if n == 2:
        raise ValueError("item two is bad")
    return n * n


def died(_item: int) -> int:
    return DIED


def square_dying_once(item: tuple[int, str]) -> int:
    """Records each call; item 0 dies the first time only, after the others finished."""
    n, folder = item
    calls = Path(folder)
    (calls / f"call-{n}-{os.getpid()}-{time.monotonic_ns()}").touch()
    marker = calls / "died-once"
    if n == 0 and not marker.exists():
        marker.touch()
        time.sleep(DEATH_DELAY_S)
        os._exit(1)
    return n * n


@pytest.fixture
def pool_sizes(monkeypatch) -> list:
    """The max_workers of every pool run_per_log creates, in order."""
    sizes: list = []

    class RecordingPool(ProcessPoolExecutor):
        def __init__(self, max_workers=None, **kwargs):
            sizes.append(max_workers)
            super().__init__(max_workers=max_workers, **kwargs)

    monkeypatch.setattr(parallel, "ProcessPoolExecutor", RecordingPool)
    return sizes


def _calls(folder: Path) -> dict[int, int]:
    counts: dict[int, int] = {}
    for path in folder.glob("call-*"):
        n = int(path.name.split("-")[1])
        counts[n] = counts.get(n, 0) + 1
    return counts


def test_results_follow_item_order():
    assert run_per_log(square, [4, 1, 3, 2], 2, died) == [16, 1, 9, 4]


def test_serial_and_parallel_agree():
    items = [5, 2, 7, 1]
    assert run_per_log(square, items, 1, died) == run_per_log(square, items, 2, died)


def test_worker_death_only_affects_its_item():
    out = run_per_log(square_or_die, [1, 2, 3, 4, 5], 2, died)
    assert out == [1, 4, DIED, 16, 25]


def test_empty_items():
    assert run_per_log(square, [], 2, died) == []


def test_finished_items_are_kept_and_the_rest_rerun_in_one_pool(tmp_path, pool_sizes):
    items = [(n, str(tmp_path)) for n in range(3)]
    assert run_per_log(square_dying_once, items, 2, died) == [0, 1, 4]
    # Items 1 and 2 finished before item 0's worker died: they are not run again.
    assert _calls(tmp_path) == {0: 2, 1: 1, 2: 1}
    # One fresh pool of `jobs` workers for the rest; nothing died there, so no item
    # runs in a pool of its own.
    assert pool_sizes == [2, 2]


def test_only_items_that_break_the_second_pool_run_alone(pool_sizes):
    out = run_per_log(square_or_die, [1, 2, 3, 4, 5], 2, died)
    assert out == [1, 4, DIED, 16, 25]
    assert pool_sizes[:2] == [2, 2]
    assert pool_sizes[2:] and set(pool_sizes[2:]) == {1}


@pytest.mark.parametrize("jobs", [1, 2])
def test_exceptions_raised_inside_fn_propagate_unchanged(jobs):
    with pytest.raises(ValueError, match="item two is bad"):
        run_per_log(raise_for_two, [1, 2, 3], jobs, died)
