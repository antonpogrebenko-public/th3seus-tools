"""Tests for run_per_log: ordering, serial/parallel parity and worker-death containment."""

from __future__ import annotations

import os

from log_validation.parallel import run_per_log

DIED = -1


def square_or_die(n: int) -> int:
    """Top-level so a spawned worker can import it."""
    if n == 3:
        os._exit(1)
    return n * n


def square(n: int) -> int:
    return n * n


def died(_item: int) -> int:
    return DIED


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
