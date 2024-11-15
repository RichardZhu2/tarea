from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
import warnings

if TYPE_CHECKING:
    from .task import Task


class Producer:
    def __init__(self, task: Task, tg: asyncio.TaskGroup):
        self.task = task
        if task.concurrency > 1:
            warnings.warn(f"The first stage of a pipeline ({task.__qualname__}) cannot be run concurrently")
        self.tg = tg
        self.q_out = asyncio.Queue(maxsize=task.throttle)
        
        self._workers_done = 0
        self._n_consumers = 1 if self.task.next is None else self.task.next.concurrency
    
    async def _job(self, *args, **kwargs):
        async for data in self.task.func(*args, **kwargs):
            await self.q_out.put(data)

        self._workers_done += 1
        if self._workers_done == self.task.concurrency:
            for _ in range(self._n_consumers):
                await self.q_out.put(None)

    def start(self, *args, **kwargs):
        self.tg.create_task(self._job(*args, **kwargs))


class ProducerConsumer:
    def __init__(self, q_in: asyncio.Queue, task: Task, tg: asyncio.TaskGroup):
        self.q_in = q_in
        self.task = task
        self.tg = tg
        self.q_out = asyncio.Queue(maxsize=task.throttle)

        self._workers_done = 0
        self._n_consumers = 1 if self.task.next is None else self.task.next.concurrency
    
    async def _job(self):
        while (data := await self.q_in.get()) is not None:
            async for output in self.task.func(data):
                await self.q_out.put(output)

        self._workers_done += 1
        if self._workers_done == self.task.concurrency:
            for _ in range(self._n_consumers):
                await self.q_out.put(None)

    def start(self):
        for _ in range(self.task.concurrency):
            self.tg.create_task(self._job())
    