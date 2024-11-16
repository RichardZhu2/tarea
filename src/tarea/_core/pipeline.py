from __future__ import annotations

import asyncio
import inspect
from typing import Callable, TYPE_CHECKING

from .util.sentinel import StopSentinel
from .stage import Producer, ProducerConsumer

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

    def __init__(self, task: Task):
        self.tasks = [task]
    
    def _get_q_out(self, tg: asyncio.TaskGroup, *args, **kwargs) -> asyncio.Queue:
        """Feed forward each stage to the next, returning the output queue of the final stage."""
        stage = Producer(task=self.tasks[0], tg=tg)
        stage.start(*args, **kwargs)
        q_out = stage.q_out

        for task in self.tasks[1:]:
            stage = ProducerConsumer(q_in=q_out, task=task, tg=tg)
            stage.start()
            q_out = stage.q_out
        
        return q_out

    async def __call__(self, *args, **kwargs):
        """Call the pipeline, taking the inputs to the first task, and returning the output from the last task."""
        try:
            async with asyncio.TaskGroup() as tg:
                output = self._get_q_out(tg, *args, **kwargs)
                while (data := await output.get()) is not StopSentinel:
                    yield data
        except ExceptionGroup as eg:
            raise eg.exceptions[0]
    
    def pipe(self, other) -> Pipeline:
        """Connect two pipelines, returning a new Pipeline."""
        if not isinstance(other, Pipeline):
            raise TypeError(f"{other} cannot be piped into a Pipeline")
        self.tasks[-1].next = other.tasks[0]
        self.tasks.extend(other.tasks)
        return self

    def __rshift__(self, other: Pipeline) -> Pipeline:
        """Allow the syntax `pipeline1 >> pipeline2`."""
        return self.pipe(other)
    
    def close(self, other: Callable) -> Callable:
        """Connect the pipeline to a sink function (a callable that takes the pipeline output as input)."""
        async def sink(*args, **kwargs):
            await other(self(*args, **kwargs))
        return sink

    def __and__(self, other: Callable) -> Callable:
        """Allow the syntax `pipeline & sink`."""
        if callable(other) and \
            (inspect.iscoroutinefunction(other) or inspect.iscoroutinefunction(other.__call__)):
            return self.close(other)
        raise TypeError(f"{other} must be an async callable that takes a generator and returns a value")
