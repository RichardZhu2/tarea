from __future__ import annotations

import inspect
from typing import Callable, TYPE_CHECKING

from .async_helper.output import AsyncPipelineOutput
from .sync_helper.output import PipelineOutput

if TYPE_CHECKING:
    from .task import Task


class Pipeline:
    """A sequence of at least 1 connected Tasks.
    
    Two pipelines can be piped into another via:
    ```python
    new_pipeline = p1 >> p2
    # OR
    new_pipeline = p1.pipe(p2)
    ```
    """

    def __new__(cls, task: Task):
        if task.is_async:
            instance = object.__new__(AsyncPipeline)
        else:
            instance = object.__new__(cls)
        instance.__init__(task=task)
        return instance

    def __init__(self, task: Task):
        self.tasks = [task]

    def __call__(self, *args, **kwargs):
        """Return the pipeline output."""
        output = PipelineOutput(self)
        return output(*args, **kwargs)
    
    def pipe(self, other) -> Pipeline:
        """Connect two pipelines, returning a new Pipeline."""
        if not isinstance(other, Pipeline):
            raise TypeError(f"{other} cannot be piped into a Pipeline")
        
        self.tasks[-1].next = other.tasks[0]
        if not isinstance(self, AsyncPipeline) and isinstance(other, AsyncPipeline):
            # piping an `AsyncPipeline` into a `Pipeline` returns an `AsyncPipeline`
            other.tasks = self.tasks + other.tasks
            return other
        self.tasks.extend(other.tasks)
        return self

    def __rshift__(self, other: Pipeline) -> Pipeline:
        """Allow the syntax `pipeline1 >> pipeline2`."""
        return self.pipe(other)
    
    def close(self, other: Callable) -> Callable:
        """Connect the pipeline to a sink function (a callable that takes the pipeline output as input)."""
        if callable(other):
            def sink(*args, **kwargs):
                return other(self(*args, **kwargs))
            return sink
        raise TypeError(f"{other} must be a callable that takes a generator and returns a value")

    def __and__(self, other: Callable) -> Callable:
        """Allow the syntax `pipeline & sink`."""
        return self.close(other)


class AsyncPipeline(Pipeline):
    def __call__(self, *args, **kwargs):
        """Return the pipeline output."""
        output = AsyncPipelineOutput(self)
        return output(*args, **kwargs)
        
    def close(self, other: Callable) -> Callable:
        """Connect the pipeline to a sink function (a callable that takes the pipeline output as input)."""
        if callable(other) and \
            (inspect.iscoroutinefunction(other) or inspect.iscoroutinefunction(other.__call__)):
            async def sink(*args, **kwargs):
                return await other(self(*args, **kwargs))
            return sink
        raise TypeError(f"{other} must be a callable that takes a generator and returns a value")
