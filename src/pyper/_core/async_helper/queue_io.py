from __future__ import annotations

from typing import TYPE_CHECKING

from ..util.sentinel import StopSentinel

if TYPE_CHECKING:
    import asyncio
    from ..task import Task


class AsyncDequeue:
    """Pulls data from an input queue."""
    def __new__(self, q_in: asyncio.Queue, task: Task):
        if task.join:
            instance = object.__new__(_JoiningAsyncDequeue)
        else:
            instance = object.__new__(_SingleAsyncDequeue)
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


class _SingleAsyncDequeue(AsyncDequeue):
    async def __call__(self):
        async for data in self._input_stream():
            yield data


class _JoiningAsyncDequeue(AsyncDequeue):
    async def __call__(self):
        yield self._input_stream()


class AsyncEnqueue:
    """Puts output from a task onto an output queue."""
    def __new__(cls, q_out: asyncio.Queue, task: Task):
        if task.is_gen:
            instance = object.__new__(_BranchingAsyncEnqueue)
        else:
            instance = object.__new__(_SingleAsyncEnqueue)
        instance.__init__(q_out=q_out, task=task)
        return instance

    def __init__(self, q_out: asyncio.Queue, task: Task):
         self.q_out = q_out
         self.task = task
        
    async def __call__(self, *args, **kwargs):
        raise NotImplementedError


class _SingleAsyncEnqueue(AsyncEnqueue):        
    async def __call__(self, *args, **kwargs):
        await self.q_out.put(await self.task.func(*args, **kwargs))


class _BranchingAsyncEnqueue(AsyncEnqueue):
    async def __call__(self, *args, **kwargs):
        async for output in self.task.func(*args, **kwargs):
            await self.q_out.put(output)
