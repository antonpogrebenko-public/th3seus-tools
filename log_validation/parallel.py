"""Run one function over many items in worker processes, surviving worker death."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import TypeVar

T = TypeVar("T")
R = TypeVar("R")

_MISSING = object()


def _run_isolated(fn: Callable[[T], R], item: T, on_death: Callable[[T], R]) -> R:
    """Run one item in its own single-worker pool; on_death if that worker dies."""
    try:
        with ProcessPoolExecutor(max_workers=1) as pool:
            return pool.submit(fn, item).result()
    except BrokenProcessPool:
        return on_death(item)


def run_per_log(
    fn: Callable[[T], R],
    items: Sequence[T],
    jobs: int,
    on_death: Callable[[T], R],
) -> list[R]:
    """Apply fn to each item and return the results in the order of items.

    With jobs > 1 the items run in a process pool. If a worker dies the pool
    breaks; every item without a collected result is then rerun alone in a
    fresh pool, and an item whose worker dies again gets on_death(item).
    Exceptions raised inside fn propagate unchanged. fn and on_death must be
    top-level functions so they pickle.
    """
    if jobs <= 1:
        return [fn(item) for item in items]

    results: list = [_MISSING] * len(items)
    with ProcessPoolExecutor(max_workers=jobs) as pool:
        futures = [pool.submit(fn, item) for item in items]
        for i, future in enumerate(futures):
            try:
                results[i] = future.result()
            except BrokenProcessPool:
                break
    return [
        _run_isolated(fn, item, on_death) if r is _MISSING else r
        for item, r in zip(items, results)
    ]
