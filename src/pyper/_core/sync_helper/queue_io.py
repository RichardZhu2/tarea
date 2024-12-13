from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Union

from ..util.sentinel import StopSentinel

if TYPE_CHECKING:
    import multiprocessing as mp
    import queue
    from ..task import Task


def DequeueFactory(q_in: Union[mp.Queue, queue.Queue], task: Task):
    return _JoiningDequeue(q_in=q_in) if task.join \
        else _SingleDequeue(q_in=q_in)


class _Dequeue:
    """Pulls data from an input queue."""
    def __init__(self, q_in: Union[mp.Queue, queue.Queue]):
        self.q_in = q_in

    def _input_stream(self):
        while (data := self.q_in.get()) is not StopSentinel:
            yield data
    
    def __call__(self):
        raise NotImplementedError


class _SingleDequeue(_Dequeue):
    def __call__(self):
        for data in self._input_stream():
            yield data


class _JoiningDequeue(_Dequeue):
    def __call__(self):
        yield self._input_stream()


def EnqueueFactory(q_out: Union[mp.Queue, queue.Queue], task: Task):
    return _BranchingEnqueue(q_out=q_out, task=task) if task.branch \
        else _SingleEnqueue(q_out=q_out, task=task)


class _Enqueue:
    """Puts output from a task onto an output queue."""
    def __init__(self, q_out: Union[mp.Queue, queue.Queue], task: Task):
        self.q_out = q_out
        self.task = task
        
    def __call__(self, *args, **kwargs):
        raise NotImplementedError


class _SingleEnqueue(_Enqueue):        
    def __call__(self, *args, **kwargs):
        self.q_out.put(self.task.func(*args, **kwargs))


class _BranchingEnqueue(_Enqueue):
    def __call__(self, *args, **kwargs):
        if isinstance(result := self.task.func(*args, **kwargs), Iterable):
            for output in result:
                self.q_out.put(output)
        else:
            raise TypeError(
                f"got object of type {type(result)} from branching task {self.task.func} which could not be iterated over."
                " (the task should be a generator, or return an iterable)")
