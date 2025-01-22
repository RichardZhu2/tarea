from __future__ import annotations

import functools
import inspect
import pickle
from typing import Callable, Dict, Optional, Tuple

from .util.unpack import unpack_args


class Task:
    """A function (or callable) which we wish to execute concurrently.
    - If the function is asynchronous, it will be executed in an `asyncio.Task`
    - If the function is synchronous, and `multiprocess=False`, it will be executed in `ThreadPoolExecutor`
    - If the function is synchronous, and `multiprocess=True`, it will be executed in `ProcessPoolExecutor`
    """

    __slots__ = [
        "func",
        "multiprocess",
        "is_async",
        "is_gen"
    ]

    def __init__(
            self,
            func: Callable,
            /,
            *,
            multiprocess: bool = False,
            unpack: bool = False,
            bind: Optional[Tuple[Tuple, Dict]] = None):
        
        if not callable(func):
            raise TypeError("A task function must be a callable object")
    
        self.is_gen = inspect.isgeneratorfunction(func) \
            or inspect.isasyncgenfunction(func) \
            or inspect.isgeneratorfunction(func.__call__) \
            or inspect.isasyncgenfunction(func.__call__)
        self.is_async = inspect.iscoroutinefunction(func) \
            or inspect.isasyncgenfunction(func) \
            or inspect.iscoroutinefunction(func.__call__) \
            or inspect.isasyncgenfunction(func.__call__)
        
        if multiprocess:
            # Asynchronous functions cannot be multiprocessed
            if self.is_async:
                raise ValueError("multiprocess cannot be True for an async task")
            
            # The function must be picklable
            try:
                pickle.dumps(func)
            except (pickle.PicklingError, AttributeError):
                raise RuntimeError(f"{func} cannot be pickled and so cannot be multiprocessed"
                    f" -- ensure that the function is globally accessible and that its definition has not changed") from None
        
        self.func = func if bind is None else functools.partial(func, *bind[0], **bind[1])
        if unpack:
            self.func = unpack_args(self.func)
        self.multiprocess = multiprocess

    
class PipelineTask(Task):
    """A task within a Pipeline. Parameters determine pipeline-related behaviour."""

    __slots__ = Task.__slots__ + [
        "branch",
        "join",
        "workers",
        "throttle"
    ]

    def __init__(
            self,
            func: Callable,
            /,
            *,
            branch: bool = False,
            join: bool = False,
            workers: int = 1,
            throttle: int = 0,
            multiprocess: bool = False,
            unpack: bool = False,
            bind: Optional[Tuple[Tuple, Dict]] = None):
        if not isinstance(workers, int):
            raise TypeError("workers must be an integer")
        if workers < 1:
            raise ValueError("workers cannot be less than 1")
        if not isinstance(throttle, int):
            raise TypeError("throttle must be an integer")
        if throttle < 0:
            raise ValueError("throttle cannot be less than 0")
        
        super().__init__(func, multiprocess=multiprocess, unpack=unpack, bind=bind)
        self.branch = branch
        self.join = join
        self.workers = workers
        self.throttle = throttle
