"""Run one function over many items in worker processes, surviving worker death."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import Future, ProcessPoolExecutor
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


def _submitted(pool: ProcessPoolExecutor, fn: Callable[[T], R],
               items: Sequence[T]) -> list[Future | None]:
    """A future per item; None for the items not submitted because the pool broke."""
    futures: list[Future | None] = []
    for item in items:
        try:
            futures.append(pool.submit(fn, item))
        except BrokenProcessPool:
            return futures + [None] * (len(items) - len(futures))
    return futures


def _outcome(future: Future | None) -> object:
    """The future's result, or _MISSING when its pool broke before it finished."""
    if future is None:
        return _MISSING
    try:
        return future.result()
    except BrokenProcessPool:
        return _MISSING


def _pool_results(fn: Callable[[T], R], items: Sequence[T], jobs: int) -> list:
    """fn of each item in one pool of `jobs` workers; _MISSING where the pool broke first.

    Every future is collected, so an item that finished after another item's worker
    died keeps its result."""
    with ProcessPoolExecutor(max_workers=jobs) as pool:
        return [_outcome(f) for f in _submitted(pool, fn, items)]


def _filled(results: list, items: Sequence[T], run: Callable[[list[T]], list]) -> list:
    """results with each _MISSING entry replaced by run(the missing items), in order."""
    missing = [item for item, r in zip(items, results) if r is _MISSING]
    if not missing:
        return results
    rerun = iter(run(missing))
    return [next(rerun) if r is _MISSING else r for r in results]


def run_per_log(
    fn: Callable[[T], R],
    items: Sequence[T],
    jobs: int,
    on_death: Callable[[T], R],
) -> list[R]:
    """Apply fn to each item and return the results in the order of items.

    With jobs > 1 the items run in a process pool. If a worker dies the pool
    breaks; the results of the items that finished are kept, and the items
    without one are rerun together in one fresh pool of `jobs` workers. Only an
    item that this second pool leaves without a result too is rerun alone in a
    single-worker pool, and gets on_death(item) if that worker dies as well.
    Exceptions raised inside fn propagate unchanged. fn and on_death must be
    top-level functions so they pickle.
    """
    if jobs <= 1:
        return [fn(item) for item in items]

    first = _pool_results(fn, items, jobs)
    second = _filled(first, items, lambda rest: _pool_results(fn, rest, jobs))
    return _filled(second, items, lambda rest: [_run_isolated(fn, item, on_death) for item in rest])
