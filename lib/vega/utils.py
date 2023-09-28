"""
Functions that are hard to name a category.
"""
from typing import Callable, Any, List
from . import log


def apply_range(*,
    func: Callable,
    start: Any,
    end: Any,
    max_batch_size: Any,
    min_batch_size: Any=None,
    ) -> List[Any]:
    """
    Apply `func` to sub-intervals of size `max_batch_size` of [start, end].
    Retrying with half batch_size if failed.
    """
    res = []
    batch_start = start
    if min_batch_size is None:
        min_batch_size = max_batch_size / 10
    while batch_start < end:
        batch_size = max_batch_size
        while batch_size >= min_batch_size:
            try:
                batch_end = min(batch_start + batch_size, end)
                log.info(f"applying {func.__name__} for sub-range {[batch_start, batch_end]}")
                res.append(func(batch_start, batch_end))
                break
            except Exception as e:
                log.error(f"failed with error: {e}")
                batch_size /= 2
                if batch_size >= min_batch_size:
                    log.info(f"retrying with smaller batch_size = {batch_size}")
                else:
                    raise Exception(f"failed with min_batch_size {min_batch_size} at batch_start = {batch_start}")
        batch_start = batch_end
    return res