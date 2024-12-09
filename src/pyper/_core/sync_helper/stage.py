from __future__ import annotations

import multiprocessing as mp
import queue
from typing import TYPE_CHECKING, Union

from .queue_io import DequeueFactory, EnqueueFactory
from ..util.sentinel import StopSentinel
from ..util.value import ThreadingValue

if TYPE_CHECKING:
    from multiprocessing.synchronize import Event as ProcessEvent
    from threading import Event as ThreadEvent
    from ..util.worker_pool import WorkerPool
    from ..task import Task


class Producer:
    def __init__(
            self,
            task: Task,
            next_task: Task,
            q_err: Union[mp.Queue, queue.Queue],
            shutdown_event: Union[ProcessEvent, ThreadEvent]):
        if task.workers > 1:
            raise RuntimeError(f"The first task in a pipeline ({task.func.__qualname__}) cannot have more than 1 worker")
        if task.join:
            raise RuntimeError(f"The first task in a pipeline ({task.func.__qualname__}) cannot join previous results")
        self.q_out = mp.Queue(maxsize=task.throttle) \
            if task.multiprocess or (next_task is not None and next_task.multiprocess) \
            else queue.Queue(maxsize=task.throttle)
        
        self._q_err = q_err
        self._shutdown_event = shutdown_event
        self._n_workers = task.workers
        self._n_consumers = 1 if next_task is None else next_task.workers
        self._enqueue = EnqueueFactory(self.q_out, task)

    def _worker(self, *args, **kwargs):
        try:
            self._enqueue(*args, **kwargs)
        except Exception as e:
            self._shutdown_event.set()
            self._q_err.put(e)
        finally:
            for _ in range(self._n_consumers):
                self.q_out.put(StopSentinel)

    def start(self, pool: WorkerPool, /, *args, **kwargs):
        pool.submit(self._worker, *args, **kwargs)


class ProducerConsumer:
    def __init__(
            self,
            q_in: Union[mp.Queue, queue.Queue],
            task: Task,
            next_task: Task,
            q_err: Union[mp.Queue, queue.Queue],
            shutdown_event: Union[ProcessEvent, ThreadEvent]):
        # The output queue is shared between this task and the next. We optimize here by using queue.Queue wherever possible
        # and only using multiprocess.Queue when the current task or the next task are multiprocessed
        self.q_out = mp.Queue(maxsize=task.throttle) \
            if task.multiprocess or (next_task is not None and next_task.multiprocess) \
            else queue.Queue(maxsize=task.throttle)
        
        self._q_err = q_err
        self._shutdown_event = shutdown_event
        self._n_workers = task.workers
        self._n_consumers = 1 if next_task is None else next_task.workers
        self._dequeue = DequeueFactory(q_in, task)
        self._enqueue = EnqueueFactory(self.q_out, task)
        self._workers_done = mp.Value('i', 0) if task.multiprocess else ThreadingValue(0)

    def _increment_workers_done(self):
        with self._workers_done.get_lock():
            self._workers_done.value += 1
            return self._workers_done.value

    def _finish(self):
        if self._increment_workers_done() == self._n_workers:
            for _ in range(self._n_consumers):
                self.q_out.put(StopSentinel)

    def _worker(self):
        try:
            for output in self._dequeue():
                if not self._shutdown_event.is_set():
                    self._enqueue(output)
        except Exception as e:
            self._shutdown_event.set()
            self._q_err.put(e)
        finally:
            self._finish()
            
    def start(self, pool: WorkerPool, /):
        for _ in range(self._n_workers):
            pool.submit(self._worker)
