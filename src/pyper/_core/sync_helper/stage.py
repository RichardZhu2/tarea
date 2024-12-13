from __future__ import annotations

import queue
import threading
from types import SimpleNamespace
from typing import TYPE_CHECKING, Union

from .queue_io import DequeueFactory, EnqueueFactory
from ..util.sentinel import StopSentinel

if TYPE_CHECKING:
    import multiprocessing as mp
    from multiprocessing.managers import SyncManager
    import multiprocessing.synchronize as mpsync
    from ..util.worker_pool import WorkerPool
    from ..task import Task


class Producer:
    def __init__(
            self,
            task: Task,
            next_task: Task,
            manager: SyncManager,
            shutdown_event: Union[mpsync.Event, threading.Event]):
        if task.workers > 1:
            raise RuntimeError(f"The first task in a pipeline ({task.func}) cannot have more than 1 worker")
        if task.join:
            raise RuntimeError(f"The first task in a pipeline ({task.func}) cannot join previous results")
        self.q_out = manager.Queue(maxsize=task.throttle) \
            if task.multiprocess or (next_task is not None and next_task.multiprocess) \
            else queue.Queue(maxsize=task.throttle)
        
        self._shutdown_event = shutdown_event
        self._n_workers = task.workers
        self._n_consumers = 1 if next_task is None else next_task.workers
        self._enqueue = EnqueueFactory(self.q_out, task)

    def _worker(self, *args, **kwargs):
        try:
            self._enqueue(*args, **kwargs)
        except Exception:
            self._shutdown_event.set()
            raise
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
            manager: SyncManager,
            shutdown_event: Union[mpsync.Event, threading.Event]):
        # The output queue is shared between this task and the next. We optimize here by using queue.Queue wherever possible
        # and only using a multiprocess Queue when the current task or the next task are multiprocessed
        self.q_out = manager.Queue(maxsize=task.throttle) \
            if task.multiprocess or (next_task is not None and next_task.multiprocess) \
            else queue.Queue(maxsize=task.throttle)
        
        self._shutdown_event = shutdown_event
        self._n_workers = task.workers
        self._n_consumers = 1 if next_task is None else next_task.workers
        self._dequeue = DequeueFactory(q_in, task)
        self._enqueue = EnqueueFactory(self.q_out, task)
        self._workers_done = manager.Value('i', 0) if task.multiprocess else SimpleNamespace(value=0)
        self._workers_done_lock = manager.Lock() if task.multiprocess else threading.Lock()

    def _worker(self):
        try:
            for output in self._dequeue():
                if not self._shutdown_event.is_set():
                    self._enqueue(output)
        except Exception:
            self._shutdown_event.set()
            raise
        finally:
            with self._workers_done_lock:
                self._workers_done.value += 1
                if self._workers_done.value == self._n_workers:
                    for _ in range(self._n_consumers):
                        self.q_out.put(StopSentinel)

    def start(self, pool: WorkerPool, /):
        for _ in range(self._n_workers):
            pool.submit(self._worker)
