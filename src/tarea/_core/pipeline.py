from __future__ import annotations

import asyncio
import inspect
from typing import Callable, Optional

from .util.sentinel import StopSentinel
from .stage import Producer, ProducerConsumer


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

    def __rshift__(self, other) -> Pipeline:
        """Allow the syntax `pipeline1 >> pipeline2`."""
        return self.pipe(other)


class Task:
    """The representation of a function used to initialize a Pipeline."""
    def __init__(
        self,
        func: Callable,
        branch: bool = False,
        join: bool = False,
        concurrency: int = 1,
        throttle: int = 0
    ):
        if not isinstance(concurrency, int):
            raise TypeError("concurrency must be an integer")
        if concurrency < 1:
            raise ValueError("concurrency cannot be less than 1")
        if not isinstance(throttle, int):
            raise TypeError("throttle must be an integer")
        if throttle < 0:
            raise ValueError("throttle cannot be less than 0")
        
        is_gen = inspect.isgeneratorfunction(func) \
            or inspect.isasyncgenfunction(func) \
            or inspect.isgeneratorfunction(func.__call__) \
            or inspect.isasyncgenfunction(func.__call__)
        if branch and not is_gen:
            raise TypeError("A branching task must exhibit generator behaviour (use the yield keyword)")
        if not branch and is_gen:
            raise TypeError("A non-branching task cannot be a generator")
        
        self.func = func
        self.branch = branch
        self.join = join
        self.concurrency = concurrency
        self.throttle = throttle

        self.next: Optional[Task] = None
