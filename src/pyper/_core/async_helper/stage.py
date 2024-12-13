from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING

from .queue_io import AsyncDequeueFactory, AsyncEnqueueFactory
from ..util.sentinel import StopSentinel

if sys.version_info < (3, 11):  # pragma: no cover
    from ..util.task_group import TaskGroup
else:
    from asyncio import TaskGroup

if TYPE_CHECKING:
    from ..task import Task


class AsyncProducer:
    def __init__(self, task: Task, next_task: Task):
        if task.workers > 1:
            raise RuntimeError(f"The first task in a pipeline ({task.func}) cannot have more than 1 worker")
        if task.join:
            raise RuntimeError(f"The first task in a pipeline ({task.func}) cannot join previous results")
        self.task = task
        self.q_out = asyncio.Queue(maxsize=task.throttle)
        
        self._n_consumers = 1 if next_task is None else next_task.workers
        self._enqueue = AsyncEnqueueFactory(self.q_out, self.task)
    
    async def _worker(self, *args, **kwargs):
        await self._enqueue(*args, **kwargs)

        for _ in range(self._n_consumers):
            await self.q_out.put(StopSentinel)

    def start(self, tg: TaskGroup, /, *args, **kwargs):
        tg.create_task(self._worker(*args, **kwargs))


class AsyncProducerConsumer:
    def __init__(self, q_in: asyncio.Queue, task: Task, next_task: Task):
        self.q_out = asyncio.Queue(maxsize=task.throttle)

        self._n_workers = task.workers
        self._n_consumers = 1 if next_task is None else next_task.workers
        self._dequeue = AsyncDequeueFactory(q_in, task)
        self._enqueue = AsyncEnqueueFactory(self.q_out, task)
        self._workers_done = 0
    
    async def _worker(self):
        async for output in self._dequeue():
            await self._enqueue(output)

        self._workers_done += 1
        if self._workers_done == self._n_workers:
            for _ in range(self._n_consumers):
                await self.q_out.put(StopSentinel)

    def start(self, tg: TaskGroup, /):
        for _ in range(self._n_workers):
            tg.create_task(self._worker())
