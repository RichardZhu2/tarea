from __future__ import annotations

import functools
import sys
import typing as t

from .pipeline import AsyncPipeline, Pipeline
from .task import Task

if sys.version_info < (3, 10):  # pragma: no cover
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec


_P = ParamSpec('P')
_R = t.TypeVar('R')
_ArgsKwargs: t.TypeAlias = t.Optional[t.Tuple[t.Tuple[t.Any], t.Dict[str, t.Any]]]


class task:
    """Decorator class to transform a function into a `Task` object, and then initialize a `Pipeline` with this task.
    A Pipeline initialized in this way consists of one Task, and can be piped into other Pipelines.

    The behaviour of each task within a Pipeline is determined by the parameters:
    * `join`: allows the function to take all previous results as input, instead of single results
    * `concurrency`: runs the functions with multiple (async or threaded) workers
    * `throttle`: limits the number of results the function is able to produce when all consumers are busy
    * `daemon`: determines whether threaded workers are daemon threads (cannot be True for async tasks)
    * `bind`: additional args and kwargs to bind to the function when defining a pipeline
    """
    @t.overload
    def __new__(
        cls,
        func: None = None,
        /,
        *,
        join: bool = False,
        concurrency: int = 1,
        throttle: int = 0,
        daemon: bool = False,
        bind: _ArgsKwargs = None
    ) -> t.Type[task]: ...
    
    @t.overload
    def __new__(
        cls,
        func: t.Callable[_P, t.Awaitable[_R]],
        /,
        *,
        join: bool = False,
        concurrency: int = 1,
        throttle: int = 0,
        daemon: bool = False,
        bind: _ArgsKwargs = None
    ) -> AsyncPipeline[_P, _R]: ...
    
    @t.overload
    def __new__(
        cls,
        func: t.Callable[_P, t.AsyncGenerator[_R]],
        /,
        *,
        join: bool = False,
        concurrency: int = 1,
        throttle: int = 0,
        daemon: bool = False,
        bind: _ArgsKwargs = None
    ) -> AsyncPipeline[_P, _R]: ...
    
    @t.overload
    def __new__(
        cls,
        func: t.Callable[_P, t.Generator[_R]],
        /,
        *,
        join: bool = False,
        concurrency: int = 1,
        throttle: int = 0,
        daemon: bool = False,
        bind: _ArgsKwargs = None
    ) -> Pipeline[_P, _R]: ...
    
    @t.overload
    def __new__(
        cls,
        func: t.Callable[_P, _R],
        /,
        *,
        join: bool = False,
        concurrency: int = 1,
        throttle: int = 0,
        daemon: bool = False,
        bind: _ArgsKwargs = None
    ) -> Pipeline[_P, _R]: ...

    def __new__(
        cls,
        func: t.Optional[t.Callable] = None,
        /,
        *,
        join: bool = False,
        concurrency: int = 1,
        throttle: int = 0,
        daemon: bool = False,
        bind: _ArgsKwargs = None
    ):
        # Classic decorator trick: @task() means func is None, @task without parentheses means func is passed. 
        if func is None:
            return functools.partial(cls, join=join, concurrency=concurrency, throttle=throttle, daemon=daemon, bind=bind)
        return Pipeline([Task(func=func, join=join, concurrency=concurrency, throttle=throttle, daemon=daemon, bind=bind)])
    
    @staticmethod
    def bind(*args, **kwargs) -> _ArgsKwargs:
        """Utility method, to be used with `functools.partial`."""
        if not args and not kwargs:
            return None
        return args, kwargs
    