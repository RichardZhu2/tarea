from __future__ import annotations

import asyncio
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
import sys
import typing as t

from .asynchronize import asynchronize
from ..task import Task

if sys.version_info < (3, 11):  # pragma: no cover
    from ..util.task_group import TaskGroup
else:
    from asyncio import TaskGroup


class TaskExecutor:
    """Facade class to simplify the instantiation of executor contexts."""
    def __init__(self):
        self.executor = None
    
    def __enter__(self) -> _TaskExecutorContext:
        self.executor = _TaskExecutorContext().__enter__()
        return self.executor
    
    def __exit__(self, et, ev, tb):
        self.executor.__exit__(et, ev, tb)
        self.executor = None

    async def __aenter__(self) -> _AsyncTaskExecutorContext:
        self.executor = await _AsyncTaskExecutorContext().__aenter__()
        return self.executor

    async def __aexit__(self, et, ev, tb):
        await self.executor.__aexit__(et, ev, tb)
        self.executor = None

    
class _TaskExecutorContext:
    """Manages the execution of Tasks in threads or processes."""
    def __init__(self):
        self.tp = ThreadPoolExecutor()
        self.pp = ProcessPoolExecutor()

    def __enter__(self) -> t.Self:
        self.tp.__enter__()
        self.pp.__enter__()
        return self
    
    def __exit__(self, et, ev, tb) -> None:
        self.pp.__exit__(et, ev, tb)
        self.tp.__exit__(et, ev, tb)

    def submit(self, task: Task, *args, **kwargs) -> Future:
        pool = self.pp if task.multiprocess else self.tp
        return pool.submit(task.func, *args, **kwargs)
    
    @staticmethod
    def gather(*futures: Future) -> t.List:
        return [future.result() for future in futures]


class _AsyncTaskExecutorContext:
    """Manages the execution of Tasks in asyncio.Tasks, threads or processes."""
    def __init__(self):
        self.tg = TaskGroup()
        self.tp = ThreadPoolExecutor()
        self.pp = ProcessPoolExecutor()

    async def __aenter__(self) -> t.Self:
        await self.tg.__aenter__()
        self.tp.__enter__()
        self.pp.__enter__()
        return self
    
    async def __aexit__(self, et, ev, tb) -> None:
        self.pp.__exit__(et, ev, tb)
        self.tp.__exit__(et, ev, tb)
        await self.tg.__aexit__(et, ev, tb)

    def submit(self, task: Task, *args, **kwargs) -> asyncio.Future:
        task = asynchronize(task, tp=self.tp, pp=self.pp)
        return asyncio.create_task(task.func(*args, **kwargs))
    
    @staticmethod
    async def gather(*futures: asyncio.Future) -> t.List:
        return await asyncio.gather(*futures)
