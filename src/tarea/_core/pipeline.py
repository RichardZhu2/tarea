from __future__ import annotations

import asyncio
# import functools
# import inspect


def task(
    func=None,
    /,
    *,
    branch: bool = False,
    join: bool = False,
    concurrency: int = 1,
    throttle: int = 0
) -> Task|Pipeline:
    # TODO Fix intellisense type hints
    """Transform a function into a Pipeline object that can be piped into other Pipelines."""
    # Classic decorator trick: @task() means func is None, @task without parentheses means func is passed. 
    if func is None:
        return Task(branch=branch, join=join, concurrency=concurrency, throttle=throttle)
    task = Task(func=func, branch=branch, join=join, concurrency=concurrency, throttle=throttle)
    return Pipeline(task)


class Task:
    """The representation of a function within of a Pipeline."""
    def __init__(
        self,
        func=None,
        branch: bool = False,
        join: bool = False,
        concurrency: int = 1,
        throttle: int = 0
    ):
        if not isinstance(concurrency, int):
            raise TypeError("concurrency must be an integer")
        if concurrency < 1:
            raise ValueError("concurrency cannot be less than 1")
        if not isinstance(throttle, int):
            raise TypeError("throttle must be an integer")
        if throttle < 0:
            raise ValueError("throttle cannot be less than 0")
        
        if func is not None:
            self.func = self.transform_func(func)
        self.branch = branch
        self.join = join
        self.concurrency = concurrency
        self.throttle = throttle

        self.next: Task = None

    def transform_func(self, func):
        return func
    
    def __call__(self, func) -> Pipeline:
        self.func = self.transform_func(func)
        return Pipeline(self)


class Pipeline:
    """A sequence of at least 1 connected Tasks.
    
    Returns a callable object with the same
    """

    def __init__(self, task: Task):
        self.tasks = [task]
    
    def _output(self, *args, **kwargs) -> asyncio.Queue:
        """Feed forward each stage to the next, returning the output queue of the final stage."""
        stage = Producer(self.tasks[0])
        stage.start(*args, **kwargs)
        q_out = stage.q_out

        for task in self.tasks[1:]:
            stage = ProducerConsumer(q_in=q_out, task=task)
            stage.start()
            q_out = stage.q_out
        
        return q_out

    async def __call__(self, *args, **kwargs):
        """"""
        #! How to handle difference between join and branch type consumers
        output = self._output(*args, **kwargs)
        while (data := await output.get()) is not None:
            yield data
    
    def pipe(self, other):
        """Connect two pipelines."""
        if not isinstance(other, Pipeline):
            raise TypeError(f"{other} cannot be piped into a Pipeline")
        self.tasks[-1].next = other.tasks[0]
        self.tasks.extend(other.tasks)
        return self

    def __rshift__(self, other):
        """Allow the syntax pipeline1 >> pipeline2."""
        return self.pipe(other)


class Producer:
    def __init__(self, task: Task):
        self.task = task
        self.q_out = asyncio.Queue(maxsize=task.throttle)
        
        self._workers = []
        self._workers_done = 0
    
    async def _job(self, *args, **kwargs):
        async for data in self.task.func(*args, **kwargs):
            await self.q_out.put(data)

        self._workers_done += 1
        if self._workers_done == self.task.concurrency:
            for _ in range(self.task.next.concurrency if self.task.next is not None else 1):
                await self.q_out.put(None)

    def start(self, *args, **kwargs):
        for _ in range(self.task.concurrency):
            self._workers.append(asyncio.create_task(self._job(*args, **kwargs)))


class ProducerConsumer:
    def __init__(self, q_in: asyncio.Queue, task: Task):
        self.q_in = q_in
        self.task = task
        self.q_out = asyncio.Queue(maxsize=task.throttle)

        self._workers_done = 0
        self._workers = []
    
    async def _job(self):
        while (data := await self.q_in.get()) is not None:
            async for output in self.task.func(data):
                await self.q_out.put(output)

        self._workers_done += 1
        if self._workers_done == self.task.concurrency:
            for _ in range(self.task.next.concurrency if self.task.next is not None else 1):
                await self.q_out.put(None)

    def start(self):
        for _ in range(self.task.concurrency):
            self._workers.append(asyncio.create_task(self._job()))
        