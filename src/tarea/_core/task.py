from __future__ import annotations

from .pipeline import Pipeline


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
