from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING

from .stage import AsyncProducer, AsyncProducerConsumer
from ..util.sentinel import StopSentinel
from ..util.thread_pool import ThreadPool

if sys.version_info < (3, 11):  # pragma: no cover
    from ..util.task_group import TaskGroup, ExceptionGroup
else:
    from asyncio import TaskGroup

if TYPE_CHECKING:
    from ..pipeline import AsyncPipeline


class AsyncPipelineOutput:
    def __init__(self, pipeline: AsyncPipeline):
        self.pipeline = pipeline

    def _get_q_out(self, tg: TaskGroup, tp: ThreadPool ,*args, **kwargs) -> asyncio.Queue:
        """Feed forward each stage to the next, returning the output queue of the final stage."""
        q_out = None
        for task, next_task in zip(self.pipeline.tasks, self.pipeline.tasks[1:] + [None]):
            n_consumers = 1 if next_task is None else next_task.concurrency
            if q_out is None:
                stage = AsyncProducer(task=self.pipeline.tasks[0], tg=tg, tp=tp, n_consumers=n_consumers)
                stage.start(*args, **kwargs)
            else:
                stage = AsyncProducerConsumer(q_in=q_out, task=task, tg=tg, tp=tp, n_consumers=n_consumers)
                stage.start()
            q_out = stage.q_out

        return q_out
    
    async def __call__(self, *args, **kwargs):
        """Call the pipeline, taking the inputs to the first task, and returning the output from the last task."""
        try:
            # We unify async and thread-based concurrency by
            # 1. using TaskGroup to spin up asynchronous tasks
            # 2. using ThreadPool to spin up synchronous tasks
            async with TaskGroup() as tg, ThreadPool() as tp:
                q_out = self._get_q_out(tg, tp, *args, **kwargs)
                while (data := await q_out.get()) is not StopSentinel:
                    yield data
        except ExceptionGroup as eg:
            raise eg.exceptions[0]
        