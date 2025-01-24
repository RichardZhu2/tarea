from __future__ import annotations

import sys
import typing as t

from .util.executor import TaskExecutor

if sys.version_info < (3, 10):  # pragma: no cover
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec
if sys.version_info < (3, 11):  # pragma: no cover
    from .util.task_group import ExceptionGroup

if t.TYPE_CHECKING:
    from .task import Task


_P = ParamSpec('P')
_R = t.TypeVar('R')


class Fork(t.Generic[_P, _R]):
    """A set of distinct, independent tasks to be gathered concurrently."""
    def __new__(cls, tasks: t.List[Task]):
        if any(task.is_async for task in tasks):
            instance = object.__new__(AsyncFork)
        else:
            instance = object.__new__(cls)
        instance.__init__(tasks=tasks)
        return instance

    def __init__(self, tasks: t.List[Task]):
        self.tasks = tasks

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> t.List:
        with TaskExecutor() as executor:
            futures = [
                executor.submit(task, *args, **kwargs)
                for task in self.tasks
            ]
            return executor.gather(*futures)

    @t.overload
    def attach(self: AsyncFork[_P, _R], other: AsyncFork) -> AsyncFork[_P, t.List]: ...
    
    @t.overload
    def attach(self: AsyncFork[_P, _R], other: Fork) -> AsyncFork[_P, t.List]: ...
    
    @t.overload
    def attach(self, other: AsyncFork) -> AsyncFork[_P, t.List]: ...
    
    @t.overload
    def attach(self, other: Fork) -> Fork[_P, t.List]: ...
    
    def attach(self, other):
        """Combine two Forks, returning a new Fork."""
        if not isinstance(other, Fork):
            raise TypeError(f"{other} of type {type(other)} cannot be attached (expected Fork object)")
        return Fork(self.tasks + other.tasks)
    
    @t.overload
    def __and__(self: AsyncFork[_P, _R], other: AsyncFork) -> AsyncFork[_P, t.List]: ...
    
    @t.overload
    def __and__(self: AsyncFork[_P, _R], other: Fork) -> AsyncFork[_P, t.List]: ...
    
    @t.overload
    def __and__(self, other: AsyncFork) -> AsyncFork[_P, t.List]: ...
    
    @t.overload
    def __and__(self, other: Fork) -> Fork[_P, t.List]: ...
    
    def __and__(self, other):
        """Combine two Forks, returning a new Fork."""
        return self.attach(other)
    
    def __repr__(self):
        return f"<{self.__class__.__name__} {[task.func for task in self.tasks]}>"
    
    def __reduce__(self):
        return (self.__class__, (self.tasks,))


class AsyncFork(Fork[_P, _R]):
    async def __call__(self, *args: _P.args, **kwargs: _P.kwargs)  -> t.List:
        try:
            async with TaskExecutor() as executor:
                futures = [
                    executor.submit(task, *args, **kwargs)
                    for task in self.tasks
                ]
                return await executor.gather(*futures)
        except ExceptionGroup as eg:
            raise eg.exceptions[0] from None
