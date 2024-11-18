from __future__ import annotations

from typing import TYPE_CHECKING

from ..util.sentinel import StopSentinel

if TYPE_CHECKING:
    import queue
    from ..task import Task


class Dequeue:
    """Pulls data from an input queue."""
    def __new__(self, q_in: queue.Queue, task: Task):
        if task.join:
            instance = object.__new__(_JoiningDequeue)
        else:
            instance = object.__new__(_SingleDequeue)
        instance.__init__(q_in=q_in, task=task)
        return instance

    def __init__(self, q_in: queue.Queue, task: Task):
         self.q_in = q_in
         self.task = task

    def _input_stream(self):
        while (data := self.q_in.get()) is not StopSentinel:
            yield data
    
    def __call__(self):
        raise NotImplementedError


class _SingleDequeue(Dequeue):
     def __call__(self):
         for data in self._input_stream():
            yield data


class _JoiningDequeue(Dequeue):
     def __call__(self):
        yield self._input_stream()


class Enqueue:
    """Puts output from a task onto an output queue."""
    def __new__(cls, q_out: queue.Queue, task: Task):
        if task.is_gen:
            instance = object.__new__(_BranchingEnqueue)
        else:
            instance = object.__new__(_SingleEnqueue)
        instance.__init__(q_out=q_out, task=task)
        return instance

    def __init__(self, q_out: queue.Queue, task: Task):
         self.q_out = q_out
         self.task = task
        
    def __call__(self, *args, **kwargs):
        raise NotImplementedError


class _SingleEnqueue(Enqueue):        
     def __call__(self, *args, **kwargs):
        self.q_out.put(self.task.func(*args, **kwargs))


class _BranchingEnqueue(Enqueue):
     def __call__(self, *args, **kwargs):
         for output in self.task.func(*args, **kwargs):
            self.q_out.put(output)
