from __future__ import annotations

import concurrent.futures as cf
import multiprocessing as mp
import multiprocessing.synchronize as mpsync
import threading
from typing import List, Union


class WorkerPool:
    """A context for fine-grained thread/process management and error handling.

    1. Spins up thread/process workers and maintains a reference to each
    2. Provides a mechanism to capture and propagate errors to the main thread/process
    3. Ensures safe tear-down of all workers 
    """
    shutdown_event: Union[mpsync.Event, threading.Event]
    _executor: Union[cf.ProcessPoolExecutor, cf.ThreadPoolExecutor]
    _futures: List[cf.Future]

    def __enter__(self):
        self._executor.__enter__()
        return self
    
    def __exit__(self, et, ev, tb):
        self._executor.__exit__(et, ev, tb)
        for future in self._futures:
            future.result()

    def submit(self, func, /, *args, **kwargs):
        future = self._executor.submit(func, *args, **kwargs)
        self._futures.append(future)
        return future


class ThreadPool(WorkerPool):
    def __init__(self):
        self.shutdown_event = threading.Event()

        self._executor = cf.ThreadPoolExecutor()
        self._futures = []


class ProcessPool(WorkerPool):
    def __init__(self):
        self.manager = mp.Manager()
        self.shutdown_event = self.manager.Event()

        self._executor = cf.ProcessPoolExecutor()
        self._futures = []
