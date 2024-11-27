from __future__ import annotations

import functools
import inspect
from typing import Callable, Dict, Optional, Tuple


class Task:
    """The representation of a function within a Pipeline."""

    __slots__ = "is_gen", "is_async", "func", "join", "concurrency", "throttle", "daemon"

    def __init__(
        self,
        func: Callable,
        join: bool = False,
        concurrency: int = 1,
        throttle: int = 0,
        daemon: bool = False,
        bind: Optional[Tuple[Tuple, Dict]] = None
    ):
        if not isinstance(concurrency, int):
            raise TypeError("concurrency must be an integer")
        if concurrency < 1:
            raise ValueError("concurrency cannot be less than 1")
        if not isinstance(throttle, int):
            raise TypeError("throttle must be an integer")
        if throttle < 0:
            raise ValueError("throttle cannot be less than 0")
        if not callable(func):
            raise TypeError("A task must be a callable object")
        
        self.is_gen = inspect.isgeneratorfunction(func) \
            or inspect.isasyncgenfunction(func) \
            or inspect.isgeneratorfunction(func.__call__) \
            or inspect.isasyncgenfunction(func.__call__)
        self.is_async = inspect.iscoroutinefunction(func) \
            or inspect.isasyncgenfunction(func) \
            or inspect.iscoroutinefunction(func.__call__) \
            or inspect.isasyncgenfunction(func.__call__)
        
        if self.is_async and daemon:
            raise ValueError("daemon cannot be True for an async task")
        
        self.func = func if bind is None else functools.partial(func, *bind[0], **bind[1])
        self.join = join
        self.concurrency = concurrency
        self.throttle = throttle
        self.daemon = daemon
