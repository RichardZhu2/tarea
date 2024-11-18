from __future__ import annotations

import functools
from typing import Callable, Dict, Optional, overload, Tuple

from .pipeline import Pipeline
from .task import Task


class task:
    """Decorator class to transform a function into a `Task` object, and then initialize a `Pipeline` with this task.
    A Pipeline initialized in this way consists of one Task, and can be piped into other Pipelines.

    The behaviour of each task within a Pipeline is determined by the parameters:
    `join`: allows the function to take all previous results as input, instead of single results
    `concurrency`: runs the functions with multiple (async or threaded) workers
    `throttle`: limits the number of results the function is able to produce when all consumers are busy
    """
    @overload
    def __new__(cls, func: None = None, /, *, join: bool = False, concurrency: int = 1, throttle: int = 0, daemon: bool = False, bind: Optional[Tuple[Tuple, Dict]] = None) -> Callable[..., Pipeline]:
        """Enable type hints for functions decorated with `@task()`."""
    
    @overload
    def __new__(cls, func: Callable, /, *, join: bool = False, concurrency: int = 1, throttle: int = 0, daemon: bool = False, bind: Optional[Tuple[Tuple, Dict]] = None) -> Pipeline:
        """Enable type hints for functions decorated with `@task`."""
    
    def __new__(
        cls,
        func: Optional[Callable] = None,
        /,
        *,
        join: bool = False,
        concurrency: int = 1,
        throttle: int = 0,
        daemon: bool = False,
        bind: Optional[Tuple[Tuple, Dict]] = None
    ):
        # Classic decorator trick: @task() means func is None, @task without parentheses means func is passed. 
        if func is None:
            return functools.partial(cls, join=join, concurrency=concurrency, throttle=throttle, daemon=daemon, bind=bind)
        return Pipeline([Task(func=func, join=join, concurrency=concurrency, throttle=throttle, daemon=daemon, bind=bind)])
    
    @staticmethod
    def bind(*args, **kwargs) -> Optional[Tuple[Tuple, Dict]]:
        """Utility method, to be used with `functools.partial`."""
        if not args and not kwargs:
            return None
        return args, kwargs
    