from __future__ import annotations

import functools
import sys
import typing as t

from .fork import AsyncFork, Fork
from .pipeline import AsyncPipeline, Pipeline
from .task import PipelineTask, Task

if sys.version_info < (3, 10):  # pragma: no cover
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec


_P = ParamSpec('P')
_R = t.TypeVar('R')
_Default = t.TypeVar('T', bound=t.NoReturn)  # Matches to no type hints
ArgsKwargs: t.TypeAlias = t.Optional[t.Tuple[t.Tuple[t.Any], t.Dict[str, t.Any]]]


class task:
    """Decorator class to initialize a `Pipeline` consisting of one task.

    Args:
        func (callable): A positional-only param defining the task function
        branch (bool): Allows the task to submit multiple outputs
        join (bool): Allows the task to take all previous results as input, instead of single results
        workers (int): Defines the number of workers to run the task
        throttle (int): Limits the number of results the task is able to produce when all consumers are busy
        multiprocess (bool): Allows the task to be multiprocessed (cannot be `True` for async tasks)
        unpack (bool): Allows unpacking of `tuple`/`list` input as *args or `dict` input as **kwargs
        bind (tuple[tuple, dict]): Additional args and kwargs to bind to the task function

    Returns:
        A `Pipeline` instance consisting of one task.

    Examples:
        ```python
        def spam(x: int):
            return x + 1

        p = task(spam)

        def ham(x: int):
            return [x, x + 1, x + 2]

        p = task(ham, branch=True, workers=10)

        async def eggs(x: int):
            yield x
            yield x + 1
            yield x + 2

        p = task(eggs, branch=True, throttle=1)
        ```
    """
    @t.overload
    def __new__(
            cls,
            func: t.Callable[_P, _Default],
            /,
            *,
            branch: bool = False,
            join: bool = False,
            workers: int = 1,
            throttle: int = 0,
            multiprocess: bool = False,
            unpack: bool = False,
            bind: ArgsKwargs = None) -> Pipeline[_P, _Default]: ...
    
    @t.overload
    def __new__(
            cls,
            func: None = None,
            /,
            *,
            branch: t.Literal[True],
            join: bool = False,
            workers: int = 1,
            throttle: int = 0,
            multiprocess: bool = False,
            unpack: bool = False,
            bind: ArgsKwargs = None) -> t.Type[_branched_partial_task]: ...
    
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
            unpack: bool = False,
            bind: ArgsKwargs = None) -> t.Type[task]: ...
    
    @t.overload
    def __new__(
            cls,
            func: t.Callable[_P, t.Union[t.Awaitable[t.Iterable[_R]], t.AsyncGenerator[_R]]],
            /,
            *,
            branch: t.Literal[True],
            join: bool = False,
            workers: int = 1,
            throttle: int = 0,
            multiprocess: bool = False,
            unpack: bool = False,
            bind: ArgsKwargs = None) -> AsyncPipeline[_P, _R]: ...
        
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
            unpack: bool = False,
            bind: ArgsKwargs = None) -> AsyncPipeline[_P, _R]: ...
        
    @t.overload
    def __new__(
            cls,
            func: t.Callable[_P, t.Iterable[_R]],
            /,
            *,
            branch: t.Literal[True],
            join: bool = False,
            workers: int = 1,
            throttle: int = 0,
            multiprocess: bool = False,
            unpack: bool = False,
            bind: ArgsKwargs = None) -> Pipeline[_P, _R]: ...
    
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
            unpack: bool = False,
            bind: ArgsKwargs = None) -> Pipeline[_P, _R]: ...

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
            unpack: bool = False,
            bind: ArgsKwargs = None):
        # Classic decorator trick: @task() means func is None, @task without parentheses means func is passed. 
        if func is None:
            return functools.partial(cls, branch=branch, join=join, workers=workers, throttle=throttle, multiprocess=multiprocess, unpack=unpack, bind=bind)
        return Pipeline([PipelineTask(func, branch=branch, join=join, workers=workers, throttle=throttle, multiprocess=multiprocess, unpack=unpack, bind=bind)])

    @t.overload
    @staticmethod
    def fork(
            func: t.Callable[_P, t.Awaitable[_R]],
            multiprocess: bool = False,
            unpack: bool = False,
            bind: ArgsKwargs = None) -> AsyncFork[_P, t.List]: ...
    
    @t.overload
    @staticmethod
    def fork(
            func: t.Callable[_P],
            multiprocess: bool = False,
            unpack: bool = False,
            bind: ArgsKwargs = None) -> Fork[_P, t.List]: ...

    @staticmethod
    def fork(
            func: t.Callable,
            /,
            *,
            multiprocess: bool = False,
            unpack: bool = False,
            bind: ArgsKwargs = None):
        """Initializes a `Fork`, representing a set of distinct, independent tasks to be run concurrently.

        Args:
            func (callable): A positional-only param defining the task function
            multiprocess (bool): Allows the task to be multiprocessed (cannot be `True` for async tasks)
            unpack (bool): Allows unpacking of `tuple`/`list` input as *args or `dict` input as **kwargs
            bind (tuple[tuple, dict]): Additional args and kwargs to bind to the task function

        Returns:
            A `Fork` instance consisting of one task.

        Examples:
            ```python
            def spam(x: int):
                return x + 1

            def ham(x: int):
                return x + 2
            
            fork = task.fork(spam) & task.fork(ham)
            result1, result2 = fork(0)
            assert result1 = 1
            assert result2 = 2
            ```
        """
        return Fork([Task(func, multiprocess=multiprocess, unpack=unpack, bind=bind)])
    
    @staticmethod
    def bind(*args, **kwargs) -> ArgsKwargs:
        """Utility method to bind additional `args` and `kwargs` to a task via the `bind` parameter.

        Example:
            ```python
            def f(x: int, y: int):
                return x + y

            p = task(f, bind=task.bind(y=1))
            p(x=1)
            ```
        """
        if args or kwargs:
            return args, kwargs


class _branched_partial_task:
    @t.overload
    def __new__(cls, func: t.Callable[_P, _Default]) -> Pipeline[_P, _Default]: ...

    @t.overload
    def __new__(
            cls,
            func: t.Callable[_P, t.Union[t.Awaitable[t.Iterable[_R]], t.AsyncGenerator[_R]]]) -> AsyncPipeline[_P, _R]: ...
        
    @t.overload
    def __new__(cls, func: t.Callable[_P, t.Iterable[_R]]) -> Pipeline[_P, _R]: ...

    @t.overload
    def __new__(cls, func: t.Callable[_P, _R]) -> Pipeline[_P, t.Any]: ...

    def __new__(cls):
        raise NotImplementedError
