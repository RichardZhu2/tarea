from __future__ import annotations

import queue
import threading
from typing import TYPE_CHECKING

from .queue_io import Dequeue, Enqueue
from ..util.sentinel import StopSentinel

if TYPE_CHECKING:
    from ..util.thread_pool import ThreadPool
    from ..task import Task


class Producer:
    def __init__(self, task: Task, tp: ThreadPool, n_consumers: int):
        self.task = task
        if task.concurrency > 1:
            raise RuntimeError(f"The first task in a pipeline ({task.func.__qualname__}) cannot have concurrency greater than 1")
        if task.join:
            raise RuntimeError(f"The first task in a pipeline ({task.func.__qualname__}) cannot join previous results")
        self.tp = tp
        self.n_consumers = n_consumers
        self.q_out = queue.Queue(maxsize=task.throttle)
        
        self._enqueue = Enqueue(self.q_out, self.task)
    
    def _worker(self, *args, **kwargs):
        try:
            self._enqueue(*args, **kwargs)
        except Exception as e:
            self.tp.put_error(e)
        finally:
            for _ in range(self.n_consumers):
                self.q_out.put(StopSentinel)

    def start(self, *args, **kwargs):
        self.tp.submit(self._worker, args, kwargs, daemon=self.task.daemon)


class ProducerConsumer:
    def __init__(self, q_in: queue.Queue, task: Task, tp: ThreadPool, n_consumers: int):
        self.q_in = q_in
        self.task = task
        self.tp = tp
        self.n_consumers = n_consumers
        self.q_out = queue.Queue(maxsize=task.throttle)

        self._workers_done = 0
        self._workers_done_lock = threading.Lock()
        self._dequeue = Dequeue(self.q_in, self.task)
        self._enqueue = Enqueue(self.q_out, self.task)
    
    def _worker(self):
        try:
            for output in self._dequeue():
                self._enqueue(output)
        except Exception as e:
            self.tp.put_error(e)
        finally:
            with self._workers_done_lock:
                self._workers_done += 1
                if self._workers_done == self.task.concurrency:
                    for _ in range(self.n_consumers):
                        self.q_out.put(StopSentinel)

    def start(self):
        for _ in range(self.task.concurrency):
            self.tp.submit(self._worker, daemon=self.task.daemon)
