from __future__ import annotations

import functools
from typing import Callable, Optional, overload

from .pipeline import Pipeline
from .task import Task


@overload
def task(func: None, /, *, branch: bool = False, join: bool = False, concurrency: int = 1, throttle: int = 0) -> Callable[..., Pipeline]:
    """Enable type hints for functions decorated with `@task()`."""


@overload
def task(func: Callable, /, *, branch: bool = False, join: bool = False, concurrency: int = 1, throttle: int = 0) -> Pipeline:
    """Enable type hints for functions decorated with `@task`."""


def task(
    func: Optional[Callable] = None,
    /,
    *,
    branch: bool = False,
    join: bool = False,
    concurrency: int = 1,
    throttle: int = 0
):
    """Transform a function into a `Task` object, then initialize a `Pipeline` object with this task.
    A Pipeline initialized in this way consists of one Task, and can be piped into other Pipelines.

    The behaviour of each task within a Pipeline is determined by the parameters:
    `branch`: allows the function to `yield` multiple results when set to `True`, instead of `return`-ing a single result
    `join`: allows the function to take all previous results as input, instead of single results
    `concurrency`: runs the functions with multiple (async or thread) workers
    `throttle`: limits the number of results the function is able to produce when all consumers are busy
    """
    # Classic decorator trick: @task() means func is None, @task without parentheses means func is passed. 
    if func is None:
        return functools.partial(task, branch=branch, join=join, concurrency=concurrency, throttle=throttle)
    return Pipeline(Task(func=func, branch=branch, join=join, concurrency=concurrency, throttle=throttle))
