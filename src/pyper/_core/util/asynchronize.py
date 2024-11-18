from __future__ import annotations

import asyncio
from concurrent.futures import Future
import functools

from .thread_pool import ThreadPool
from ..task import Task


def ascynchronize(task: Task, tp: ThreadPool) -> Task:
    """Unifies async and sync functions within an `AsyncPipeline`.

    Synchronous functions within a `ThreadPool` are transformed into asynchronous functions via `asyncio.wrap_future`.
    Synchronous generators are transformed into asynchronous generators.
    """
    if task.is_async:
        return task
    
    if task.is_gen:
        @functools.wraps(task.func)
        async def wrapper(*args, **kwargs):
            for output in task.func(*args, **kwargs):
                yield output
    else:
        @functools.wraps(task.func)
        async def wrapper(*args, **kwargs):
            future = Future()
            def target(*args, **kwargs):
                try:
                    result = task.func(*args, **kwargs)
                    future.set_result(result)
                except Exception as e:
                    future.set_exception(e)
            tp.submit(target, args=args, kwargs=kwargs, daemon=task.daemon)
            return await asyncio.wrap_future(future)
    
    return Task(
        func=wrapper,
        join=task.join,
        concurrency=task.concurrency,
        throttle=task.throttle
    )
