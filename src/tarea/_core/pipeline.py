from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from .stage import Producer, ProducerConsumer

if TYPE_CHECKING:
    from .task import Task


class Pipeline:
    """A sequence of at least 1 connected Tasks.
    
    Returns a callable object with the same signature as the initial task.
    """

    def __init__(self, task: Task):
        self.tasks = [task]
    
    def _output(self, tg, *args, **kwargs) -> asyncio.Queue:
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
        #! How to handle difference between join and branch type consumers
        try:
            async with asyncio.TaskGroup() as tg:
                output = self._output(tg, *args, **kwargs)
                while (data := await output.get()) is not None:
                    yield data
        except ExceptionGroup as eg:
            raise eg.exceptions[0]
    
    def pipe(self, other) -> Pipeline:
        """Connect two pipelines."""
        if not isinstance(other, Pipeline):
            raise TypeError(f"{other} cannot be piped into a Pipeline")
        self.tasks[-1].next = other.tasks[0]
        self.tasks.extend(other.tasks)
        return self

    def __rshift__(self, other) -> Pipeline:
        """Allow the syntax pipeline1 >> pipeline2."""
        return self.pipe(other)
