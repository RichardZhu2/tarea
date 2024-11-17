from __future__ import annotations

import asyncio
import functools

from ..task import Task


def ascynchronize(task: Task) -> Task:
    """Unifies async and sync functions within an `AsyncPipeline`.

    Synchronous functions are transformed into asynchronous functions via `loop.run_in_executor`.
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
            loop = asyncio.get_running_loop()
            job = functools.partial(task.func, *args, **kwargs)
            return await loop.run_in_executor(None, job)
    
    return Task(
        func=wrapper,
        branch=task.branch,
        join=task.join,
        concurrency=task.concurrency,
        throttle=task.throttle
    )
