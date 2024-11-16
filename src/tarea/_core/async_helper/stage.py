from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from .queue_io import AsyncDequeue, AsyncEnqueue
from ..util.sentinel import StopSentinel

if TYPE_CHECKING:
    from ..task import Task


class AsyncProducer:
    def __init__(self, task: Task, tg: asyncio.TaskGroup):
        self.task = task
        if task.concurrency > 1:
            raise RuntimeError(f"The first task in a pipeline ({task.func.__qualname__}) cannot have concurrency greater than 1")
        if task.join:
            raise RuntimeError(f"The first task in a pipeline ({task.func.__qualname__}) cannot join previous results")
        self.tg = tg
        self.q_out = asyncio.Queue(maxsize=task.throttle)
        
        self._n_consumers = 1 if self.task.next is None else self.task.next.concurrency
        self._enqueue = AsyncEnqueue(self.q_out, self.task)
    
    async def _job(self, *args, **kwargs):
        await self._enqueue(*args, **kwargs)

        for _ in range(self._n_consumers):
            await self.q_out.put(StopSentinel)

    def start(self, *args, **kwargs):
        self.tg.create_task(self._job(*args, **kwargs))


class AsyncProducerConsumer:
    def __init__(self, q_in: asyncio.Queue, task: Task, tg: asyncio.TaskGroup):
        self.q_in = q_in
        self.task = task
        self.tg = tg
        self.q_out = asyncio.Queue(maxsize=task.throttle)

        self._workers_done = 0
        self._n_consumers = 1 if self.task.next is None else self.task.next.concurrency
        self._dequeue = AsyncDequeue(self.q_in, self.task)
        self._enqueue = AsyncEnqueue(self.q_out, self.task)
    
    async def _job(self):
        async for output in self._dequeue():
            await self._enqueue(output)

        self._workers_done += 1
        if self._workers_done == self.task.concurrency:
            for _ in range(self._n_consumers):
                await self.q_out.put(StopSentinel)

    def start(self):
        for _ in range(self.task.concurrency):
            self.tg.create_task(self._job())
