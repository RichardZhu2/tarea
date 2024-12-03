from __future__ import annotations

import asyncio
from concurrent.futures import ProcessPoolExecutor, Future
import functools

from .thread_pool import ThreadPool
from ..task import Task


def ascynchronize(task: Task, tp: ThreadPool, pp: ProcessPoolExecutor) -> Task:
    """Unifies async and sync tasks as awaitable futures.
    1. If the task is async already, return it.
    2. Synchronous generators are transformed into asynchronous generators.
    3. Multiprocessed synchronous functions within a `ProcessPoolExecutor` are wrapped in `asyncio.wrap_future`.
    4. Threaded synchronous functions within a `ThreadPool` are wrapped in `asyncio.wrap_future`.
    """
    if task.is_async:
        return task  
    if task.is_gen:
        @functools.wraps(task.func)
        async def wrapper(*args, **kwargs):
            for output in task.func(*args, **kwargs):
                yield output
    elif task.multiprocess:
        @functools.wraps(task.func)
        async def wrapper(*args, **kwargs):
            loop = asyncio.get_running_loop()
            f = functools.partial(task.func, *args, **kwargs)
            return await loop.run_in_executor(executor=pp, func=f)
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
