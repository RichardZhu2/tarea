from __future__ import annotations

import multiprocessing as mp
import queue
import threading
from typing import List, Union


class WorkerPool:
    """A context for fine-grained thread/process management and error handling.

    1. Spins up thread/process workers and maintains a reference to each
    2. Provides a mechanism to capture and propagate errors to the main thread/process
    3. Ensures safe tear-down of all workers 
    """
    worker_type = None

    def __init__(self):
        self.error_queue = mp.Queue() if self.worker_type is mp.Process else queue.Queue()
        self.shutdown_event = mp.Event() if self.worker_type is mp.Process else threading.Event()

        self._workers: List[Union[mp.Process, threading.Thread]] = []

    def __enter__(self):
        return self
    
    def __exit__(self, et, ev, tb):
        for worker in self._workers:
            worker.join()

    @property
    def has_error(self):
        return not self.error_queue.empty()
    
    def get_error(self) -> Exception:
        return self.error_queue.get()

    def raise_error_if_exists(self):
        if self.has_error:
            raise self.get_error() from None

    def submit(self, func, /, *args, **kwargs):
        w = self.worker_type(target=func, args=args, kwargs=kwargs)
        w.start()
        self._workers.append(w)


class ThreadPool(WorkerPool):
    worker_type = threading.Thread


class ProcessPool(WorkerPool):
    worker_type = mp.Process
