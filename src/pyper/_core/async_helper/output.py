from __future__ import annotations

import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import sys
from typing import TYPE_CHECKING

from .stage import AsyncProducer, AsyncProducerConsumer
from ..util.asynchronize import ascynchronize
from ..util.sentinel import StopSentinel

if sys.version_info < (3, 11):  # pragma: no cover
    from ..util.task_group import TaskGroup, ExceptionGroup
else:
    from asyncio import TaskGroup

if TYPE_CHECKING:
    from ..pipeline import AsyncPipeline


class AsyncPipelineOutput:
    def __init__(self, pipeline: AsyncPipeline):
        self.pipeline = pipeline

    def _get_q_out(self, tg: TaskGroup, tp: ThreadPoolExecutor, pp: ProcessPoolExecutor, *args, **kwargs) -> asyncio.Queue:
        """Feed forward each stage to the next, returning the output queue of the final stage."""
        q_out = None
        for task, next_task in zip(self.pipeline.tasks, self.pipeline.tasks[1:] + [None]):
            task = ascynchronize(task, tp=tp, pp=pp)
            if q_out is None:
                stage = AsyncProducer(task=task, next_task=next_task)
                stage.start(tg, *args, **kwargs)
            else:
                stage = AsyncProducerConsumer(q_in=q_out, task=task, next_task=next_task)
                stage.start(tg)
            q_out = stage.q_out

        return q_out
    
    async def __call__(self, *args, **kwargs):
        """Iterate through the pipeline, taking the inputs to the first task, and yielding each output from the last task.

        Unify async, threaded, and multiprocessed work by:
        1. using TaskGroup to execute asynchronous tasks
        2. using ThreadPoolExecutor to execute threaded synchronous tasks
        3. using ProcessPoolExecutor to execute multiprocessed synchronous tasks
        """
        try:
            async with TaskGroup() as tg:
                with ThreadPoolExecutor() as tp, ProcessPoolExecutor() as pp:
                    q_out = self._get_q_out(tg, tp, pp, *args, **kwargs)
                    while (data := await q_out.get()) is not StopSentinel:
                        yield data
        except ExceptionGroup as eg:
            raise eg.exceptions[0] from None
        