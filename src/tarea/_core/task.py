from __future__ import annotations

import inspect
from typing import Callable, Optional


class Task:
    """The representation of a function used to initialize a Pipeline."""
    def __init__(
        self,
        func: Callable,
        branch: bool = False,
        join: bool = False,
        concurrency: int = 1,
        throttle: int = 0
    ):
        if not isinstance(concurrency, int):
            raise TypeError("concurrency must be an integer")
        if concurrency < 1:
            raise ValueError("concurrency cannot be less than 1")
        if not isinstance(throttle, int):
            raise TypeError("throttle must be an integer")
        if throttle < 0:
            raise ValueError("throttle cannot be less than 0")
        
        is_gen = inspect.isgeneratorfunction(func) \
            or inspect.isasyncgenfunction(func) \
            or inspect.isgeneratorfunction(func.__call__) \
            or inspect.isasyncgenfunction(func.__call__)
        if branch and not is_gen:
            raise TypeError("A branching task must exhibit generator behaviour (use the yield keyword)")
        if not branch and is_gen:
            raise TypeError("A non-branching task cannot be a generator")
        
        self.func = func
        self.branch = branch
        self.join = join
        self.concurrency = concurrency
        self.throttle = throttle

        self.next: Optional[Task] = None
