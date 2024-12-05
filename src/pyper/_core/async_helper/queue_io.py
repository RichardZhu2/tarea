from __future__ import annotations

from typing import TYPE_CHECKING

from ..util.sentinel import StopSentinel

if TYPE_CHECKING:
    import asyncio
    from ..task import Task


def AsyncDequeueFactory(q_in: asyncio.Queue, task: Task):
    return _JoiningAsyncDequeue(q_in=q_in) if task.join \
        else _SingleAsyncDequeue(q_in=q_in)


class _AsyncDequeue:
    """Pulls data from an input queue."""
    def __init__(self, q_in: asyncio.Queue):
        self.q_in = q_in

    async def _input_stream(self):
        while (data := await self.q_in.get()) is not StopSentinel:
            yield data
    
    def __call__(self):
        raise NotImplementedError


class _SingleAsyncDequeue(_AsyncDequeue):
    async def __call__(self):
        async for data in self._input_stream():
            yield data


class _JoiningAsyncDequeue(_AsyncDequeue):
    async def __call__(self):
        yield self._input_stream()


def AsyncEnqueueFactory(q_out: asyncio.Queue, task: Task):
    return _BranchingAsyncEnqueue(q_out=q_out, task=task) if task.is_gen \
        else _SingleAsyncEnqueue(q_out=q_out, task=task)


class _AsyncEnqueue:
    """Puts output from a task onto an output queue."""
    def __init__(self, q_out: asyncio.Queue, task: Task):
        self.q_out = q_out
        self.task = task
        
    async def __call__(self, *args, **kwargs):
        raise NotImplementedError


class _SingleAsyncEnqueue(_AsyncEnqueue):        
    async def __call__(self, *args, **kwargs):
        await self.q_out.put(await self.task.func(*args, **kwargs))


class _BranchingAsyncEnqueue(_AsyncEnqueue):
    async def __call__(self, *args, **kwargs):
        async for output in self.task.func(*args, **kwargs):
            await self.q_out.put(output)
