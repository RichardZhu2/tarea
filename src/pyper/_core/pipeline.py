from __future__ import annotations

import inspect
import sys
import typing as t

from .async_helper.output import AsyncPipelineOutput
from .sync_helper.output import PipelineOutput

if sys.version_info < (3, 10):  # pragma: no cover
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

if t.TYPE_CHECKING:
    from .task import Task


_P = ParamSpec('P')
_R = t.TypeVar('R')
_P_Other = ParamSpec("P_Other")
_R_Other = t.TypeVar("R_Other")


class Pipeline(t.Generic[_P, _R]):
    """A sequence of at least 1 Tasks.
    
    Two pipelines can be piped into another via:
    ```python
    new_pipeline = p1 | p2
    # OR
    new_pipeline = p1.pipe(p2)
    ```
    """

    def __new__(cls, tasks: t.List[Task]):
        if any(task.is_async for task in tasks):
            instance = object.__new__(AsyncPipeline)
        else:
            instance = object.__new__(cls)
        instance.__init__(tasks=tasks)
        return instance

    def __init__(self, tasks: t.List[Task]):
        self.tasks = tasks

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> t.Generator[_R]:
        """Return the pipeline output."""
        output = PipelineOutput(self)
        return output(*args, **kwargs)

    @t.overload
    def pipe(self: AsyncPipeline[_P, _R], other: AsyncPipeline[_P_Other, _R_Other]) -> AsyncPipeline[_P, _R_Other]: ...
    
    @t.overload
    def pipe(self: AsyncPipeline[_P, _R], other: Pipeline[_P_Other, _R_Other]) -> AsyncPipeline[_P, _R_Other]: ...
    
    @t.overload
    def pipe(self, other: AsyncPipeline[_P_Other, _R_Other]) -> AsyncPipeline[_P, _R_Other]: ...
    
    @t.overload
    def pipe(self, other: Pipeline[_P_Other, _R_Other]) -> Pipeline[_P, _R_Other]: ...
    
    def pipe(self, other: Pipeline):
        """Connect two pipelines, returning a new Pipeline."""
        if not isinstance(other, Pipeline):
            raise TypeError(f"{other} of type {type(other)} cannot be piped into a Pipeline")
        return Pipeline(self.tasks + other.tasks)

    @t.overload
    def __or__(self: AsyncPipeline[_P, _R], other: AsyncPipeline[_P_Other, _R_Other]) -> AsyncPipeline[_P, _R_Other]: ...
    
    @t.overload
    def __or__(self: AsyncPipeline[_P, _R], other: Pipeline[_P_Other, _R_Other]) -> AsyncPipeline[_P, _R_Other]: ...
    
    @t.overload
    def __or__(self, other: AsyncPipeline[_P_Other, _R_Other]) -> AsyncPipeline[_P, _R_Other]: ...
    
    @t.overload
    def __or__(self, other: Pipeline[_P_Other, _R_Other]) -> Pipeline[_P, _R_Other]: ...

    def __or__(self, other: Pipeline):
        """Connect two pipelines, returning a new Pipeline."""
        return self.pipe(other)
    
    def consume(self, other: t.Callable[..., _R_Other]) -> t.Callable[_P, _R_Other]:
        """Connect the pipeline to a consumer function (a callable that takes the pipeline output as input)."""
        if callable(other):
            def consumer(*args: _P.args, **kwargs: _P.kwargs) -> _R_Other:
                return other(self(*args, **kwargs))
            return consumer
        raise TypeError(f"{other} must be a callable that takes a generator")

    def __gt__(self, other: t.Callable[..., _R_Other]) -> t.Callable[_P, _R_Other]:
        """Connect the pipeline to a consumer function (a callable that takes the pipeline output as input)."""
        return self.consume(other)
    
    def __repr__(self):
        return f"<{self.__class__.__name__} {[task.func for task in self.tasks]}>"


class AsyncPipeline(Pipeline[_P, _R]):
    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> t.AsyncGenerator[_R]:
        """Return the pipeline output."""
        output = AsyncPipelineOutput(self)
        return output(*args, **kwargs)
    
    def consume(self, other: t.Callable[..., _R_Other]) -> t.Callable[_P, _R_Other]:
        """Connect the pipeline to a consumer function (a callable that takes the pipeline output as input)."""
        if callable(other) and \
            (inspect.iscoroutinefunction(other) or inspect.iscoroutinefunction(other.__call__)):
            async def consumer(*args: _P.args, **kwargs: _P.kwargs) -> _R_Other:
                return await other(self(*args, **kwargs))
            return consumer
        raise TypeError(f"{other} must be an async callable that takes an async generator")
