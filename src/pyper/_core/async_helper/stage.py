from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING

from .queue_io import AsyncDequeue, AsyncEnqueue
from ..util.asynchronize import ascynchronize
from ..util.sentinel import StopSentinel

if sys.version_info < (3, 11):  # pragma: no cover
    from ..util.task_group import TaskGroup
else:
    from asyncio import TaskGroup

if TYPE_CHECKING:
    from ..util.thread_pool import ThreadPool
    from ..task import Task


class AsyncProducer:
    def __init__(self, task: Task, tg: TaskGroup, tp: ThreadPool, n_consumers: int):
        self.task = ascynchronize(task, tp)
        if task.concurrency > 1:
            raise RuntimeError(f"The first task in a pipeline ({task.func.__qualname__}) cannot have concurrency greater than 1")
        if task.join:
            raise RuntimeError(f"The first task in a pipeline ({task.func.__qualname__}) cannot join previous results")
        self.tg = tg
        self.n_consumers = n_consumers
        self.q_out = asyncio.Queue(maxsize=task.throttle)
        
        self._enqueue = AsyncEnqueue(self.q_out, self.task)
    
    async def _worker(self, *args, **kwargs):
        await self._enqueue(*args, **kwargs)

        for _ in range(self.n_consumers):
            await self.q_out.put(StopSentinel)

    def start(self, *args, **kwargs):
        self.tg.create_task(self._worker(*args, **kwargs))


class AsyncProducerConsumer:
    def __init__(self, q_in: asyncio.Queue, task: Task, tg: TaskGroup, tp: ThreadPool, n_consumers: int):
        self.q_in = q_in
        self.task = ascynchronize(task, tp)
        self.tg = tg
        self.n_consumers = n_consumers
        self.q_out = asyncio.Queue(maxsize=task.throttle)

        self._workers_done = 0
        self._dequeue = AsyncDequeue(self.q_in, self.task)
        self._enqueue = AsyncEnqueue(self.q_out, self.task)
    
    async def _worker(self):
        async for output in self._dequeue():
            await self._enqueue(output)

        self._workers_done += 1
        if self._workers_done == self.task.concurrency:
            for _ in range(self.n_consumers):
                await self.q_out.put(StopSentinel)

    def start(self):
        for _ in range(self.task.concurrency):
            self.tg.create_task(self._worker())
