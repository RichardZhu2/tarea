from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from .util.sentinel import StopSentinel

if TYPE_CHECKING:
    from .task import Task


class Producer:
    def __init__(self, task: Task, tg: asyncio.TaskGroup):
        self.task = task
        if task.concurrency > 1:
            raise RuntimeError(f"The first task in a pipeline ({task.func.__qualname__}) cannot have concurrency greater than 1")
        if task.join:
            raise RuntimeError(f"The first task in a pipeline ({task.func.__qualname__}) cannot join previous results")
        self.tg = tg
        self.q_out = asyncio.Queue(maxsize=task.throttle)
        
        self._n_consumers = 1 if self.task.next is None else self.task.next.concurrency
        self._enqueue = Enqueue(self.q_out, self.task)
    
    async def _job(self, *args, **kwargs):
        await self._enqueue(*args, **kwargs)

        for _ in range(self._n_consumers):
            await self.q_out.put(StopSentinel)

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
        self._dequeue = Dequeue(self.q_in, self.task)
        self._enqueue = Enqueue(self.q_out, self.task)
    
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
    

class Dequeue:
    """Pulls data from an input queue."""
    def __new__(self, q_in: asyncio.Queue, task: Task):
        if task.join:
            instance = object.__new__(_JoiningDequeue)
        else:
            instance = object.__new__(_SingleDequeue)
        instance.__init__(q_in=q_in, task=task)
        return instance

    def __init__(self, q_in: asyncio.Queue, task: Task):
         self.q_in = q_in
         self.task = task

    async def _input_stream(self):
        while (data := await self.q_in.get()) is not StopSentinel:
            yield data
    
    def __call__(self):
        raise NotImplementedError


class _SingleDequeue(Dequeue):
    async def __call__(self):
        async for data in self._input_stream():
            yield data


class _JoiningDequeue(Dequeue):
    async def __call__(self):
        yield self._input_stream()


class Enqueue:
    """Puts output from a task onto an output queue."""
    def __new__(cls, q_out: asyncio.Queue, task: Task):
        if task.branch:
            instance = object.__new__(_BranchingEnqueue)
        else:
            instance = object.__new__(_SingleEnqueue)
        instance.__init__(q_out=q_out, task=task)
        return instance

    def __init__(self, q_out: asyncio.Queue, task: Task):
         self.q_out = q_out
         self.task = task
        
    async def __call__(self, *args, **kwargs):
        raise NotImplementedError


class _SingleEnqueue(Enqueue):        
    async def __call__(self, *args, **kwargs):
        await self.q_out.put(await self.task.func(*args, **kwargs))


class _BranchingEnqueue(Enqueue):
    async def __call__(self, *args, **kwargs):
        async for output in self.task.func(*args, **kwargs):
            await self.q_out.put(output)
