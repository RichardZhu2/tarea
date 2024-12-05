from __future__ import annotations

import multiprocessing as mp
import queue
import threading
from typing import TYPE_CHECKING, Union

from .queue_io import DequeueFactory, EnqueueFactory
from ..util.sentinel import StopSentinel

if TYPE_CHECKING:
    from ..util.worker_pool import WorkerPool
    from ..task import Task


class Producer:
    def __init__(self, task: Task, next_task: Task, q_err: Union[mp.Queue, queue.Queue]):
        if task.concurrency > 1:
            raise RuntimeError(f"The first task in a pipeline ({task.func.__qualname__}) cannot have concurrency greater than 1")
        if task.join:
            raise RuntimeError(f"The first task in a pipeline ({task.func.__qualname__}) cannot join previous results")
        self.q_out = mp.Queue(maxsize=task.throttle) \
            if task.multiprocess or (next_task is not None and next_task.multiprocess) \
            else queue.Queue(maxsize=task.throttle)
        
        self._q_err = q_err
        self._n_workers = task.concurrency
        self._n_consumers = 1 if next_task is None else next_task.concurrency
        self._enqueue = EnqueueFactory(self.q_out, task)

        self._multiprocess = task.multiprocess
    
    def _worker(self, *args, **kwargs):
        try:
            self._enqueue(*args, **kwargs)
        except Exception as e:
            self._q_err.put(e)
        finally:
            for _ in range(self._n_consumers):
                self.q_out.put(StopSentinel)

    def start(self, pool: WorkerPool, /, *args, **kwargs):
        pool.submit(self._worker, *args, **kwargs)


class ProducerConsumer:
    def __init__(self, q_in: Union[mp.Queue, queue.Queue], task: Task, next_task: Task, q_err: Union[mp.Queue, queue.Queue]):
        self.q_out = mp.Queue(maxsize=task.throttle) \
            if task.multiprocess or (next_task is not None and next_task.multiprocess) \
            else queue.Queue(maxsize=task.throttle)
        
        self._q_err = q_err
        self._n_workers = task.concurrency
        self._n_consumers = 1 if next_task is None else next_task.concurrency
        self._dequeue = DequeueFactory(q_in, task)
        self._enqueue = EnqueueFactory(self.q_out, task)

        self._multiprocess = task.multiprocess
        if self._multiprocess:
            self._workers_done = mp.Value('i', 0)
            self._lock = self._workers_done.get_lock()
        else:
            self._workers_done = 0
            self._lock = threading.Lock()

    def _increment_workers_done(self):
        with self._lock:
            if self._multiprocess:
                self._workers_done.value += 1
                return self._workers_done.value
            else:
                self._workers_done += 1
                return self._workers_done

    def _finish(self):
        if self._increment_workers_done() == self._n_workers:
            for _ in range(self._n_consumers):
                self.q_out.put(StopSentinel)

    def _worker(self):
        try:
            for output in self._dequeue():
                self._enqueue(output)
        except Exception as e:
            self._q_err.put(e)
        finally:
            self._finish()
            
    def start(self, pool: WorkerPool, /):
        for _ in range(self._n_workers):
            pool.submit(self._worker)
