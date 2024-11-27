from __future__ import annotations

import inspect
from typing import Callable, List, TYPE_CHECKING

from .async_helper.output import AsyncPipelineOutput
from .sync_helper.output import PipelineOutput

if TYPE_CHECKING:
    from .task import Task


class Pipeline:
    """A sequence of at least 1 Tasks.
    
    Two pipelines can be piped into another via:
    ```python
    new_pipeline = p1 | p2
    # OR
    new_pipeline = p1.pipe(p2)
    ```
    """

    def __new__(cls, tasks: List[Task]):
        if any(task.is_async for task in tasks):
            instance = object.__new__(AsyncPipeline)
        else:
            instance = object.__new__(cls)
        instance.__init__(tasks=tasks)
        return instance

    def __init__(self, tasks: List[Task]):
        self.tasks = tasks

    def __call__(self, *args, **kwargs):
        """Return the pipeline output."""
        output = PipelineOutput(self)
        return output(*args, **kwargs)
    
    def pipe(self, other) -> Pipeline:
        """Connect two pipelines, returning a new Pipeline."""
        if not isinstance(other, Pipeline):
            raise TypeError(f"{other} of type {type(other)} cannot be piped into a Pipeline")
        return Pipeline(self.tasks + other.tasks)

    def __or__(self, other: Pipeline) -> Pipeline:
        """Allow the syntax `pipeline1 | pipeline2`."""
        return self.pipe(other)
    
    def consume(self, other: Callable) -> Callable:
        """Connect the pipeline to a consumer function (a callable that takes the pipeline output as input)."""
        if callable(other):
            def consumer(*args, **kwargs):
                return other(self(*args, **kwargs))
            return consumer
        raise TypeError(f"{other} must be a callable that takes a generator")

    def __gt__(self, other: Callable) -> Callable:
        """Allow the syntax `pipeline > consumer`."""
        return self.consume(other)
    
    def __repr__(self):
        return f"{self.__class__.__name__} {[task.func for task in self.tasks]}"


class AsyncPipeline(Pipeline):
    def __call__(self, *args, **kwargs):
        """Return the pipeline output."""
        output = AsyncPipelineOutput(self)
        return output(*args, **kwargs)
        
    def consume(self, other: Callable) -> Callable:
        """Connect the pipeline to a consumer function (a callable that takes the pipeline output as input)."""
        if callable(other) and \
            (inspect.iscoroutinefunction(other) or inspect.iscoroutinefunction(other.__call__)):
            async def consumer(*args, **kwargs):
                return await other(self(*args, **kwargs))
            return consumer
        raise TypeError(f"{other} must be an async callable that takes an async generator")
