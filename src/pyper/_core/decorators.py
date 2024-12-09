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
    """Decorator class to initialize a `Pipeline` consisting of one task.

    Args:
        func (callable): A positional-only param defining the task function (can be omitted when using `@task`)
        branch (bool): Allows the task to submit multiple outputs
        join (bool): Allows the task to take all previous results as input, instead of single results
        workers (int): Defines the number of workers to run the task
        throttle (int): Limits the number of results the task is able to produce when all consumers are busy
        multiprocess (bool): Allows the task to be multiprocessed (cannot be `True` for async tasks)
        bind (tuple[args, kwargs]): Additional args and kwargs to bind to the task when defining a pipeline

    Returns:
        Pipeline: A `Pipeline` instance consisting of one task.

    Example:
    ```python
    def f(x: int):
        return x + 1

    p = task(f, workers=10, multiprocess=True)
    ```
    """
    @t.overload
    def __new__(
            cls,
            func: None = None,
            /,
            *,
            branch: bool = False,
            join: bool = False,
            workers: int = 1,
            throttle: int = 0,
            multiprocess: bool = False,
            bind: _ArgsKwargs = None) -> t.Type[task]: ...
    
    @t.overload
    def __new__(
            cls,
            func: t.Callable[_P, t.Union[t.Awaitable[t.Iterable[_R]], t.AsyncGenerator[_R]]],
            /,
            *,
            branch: True,
            join: bool = False,
            workers: int = 1,
            throttle: int = 0,
            multiprocess: bool = False,
            bind: _ArgsKwargs = None) -> AsyncPipeline[_P, _R]: ...
        
    @t.overload
    def __new__(
            cls,
            func: t.Callable[_P, t.Awaitable[_R]],
            /,
            *,
            branch: bool = False,
            join: bool = False,
            workers: int = 1,
            throttle: int = 0,
            multiprocess: bool = False,
            bind: _ArgsKwargs = None) -> AsyncPipeline[_P, _R]: ...
        
    @t.overload
    def __new__(
            cls,
            func: t.Callable[_P, t.Iterable[_R]],
            /,
            *,
            branch: True,
            join: bool = False,
            workers: int = 1,
            throttle: int = 0,
            multiprocess: bool = False,
            bind: _ArgsKwargs = None) -> Pipeline[_P, _R]: ...
    
    @t.overload
    def __new__(
            cls,
            func: t.Callable[_P, _R],
            /,
            *,
            branch: bool = False,
            join: bool = False,
            workers: int = 1,
            throttle: int = 0,
            multiprocess: bool = False,
            bind: _ArgsKwargs = None) -> Pipeline[_P, _R]: ...

    def __new__(
            cls,
            func: t.Optional[t.Callable] = None,
            /,
            *,
            branch: bool = False,
            join: bool = False,
            workers: int = 1,
            throttle: int = 0,
            multiprocess: bool = False,
            bind: _ArgsKwargs = None):
        # Classic decorator trick: @task() means func is None, @task without parentheses means func is passed. 
        if func is None:
            return functools.partial(cls, branch=branch, join=join, workers=workers, throttle=throttle, multiprocess=multiprocess, bind=bind)
        return Pipeline([Task(func=func, branch=branch, join=join, workers=workers, throttle=throttle, multiprocess=multiprocess, bind=bind)])
    
    @staticmethod
    def bind(*args, **kwargs) -> _ArgsKwargs:
        """Bind additional `args` and `kwargs` to a task.

        Example:
        ```python
        def f(x: int, y: int):
            return x + y

        p = task(f, bind=task.bind(y=1))
        p(x=1)
        ```
        """
        if not args and not kwargs:
            return None
        return args, kwargs
