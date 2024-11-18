from __future__ import annotations

import functools
import queue
import threading
from typing import List


class ThreadPool:
    """Handles spinning up and joining threads while providing a way to capture errors and propagate them upwards."""
    def __init__(self):
        self._threads: List[threading.Thread] = []
        self._error_queue = queue.Queue()
    
    def __enter__(self):
        return self
    
    def __exit__(self, ev, et, tb):
        for thread in self._threads:
            thread.daemon or thread.join()

    async def __aenter__(self):
        return self
    
    async def __aexit__(self, ev, et, tb):
        for thread in self._threads:
            thread.daemon or thread.join()

    @property
    def has_error(self):
        return not self._error_queue.empty()
    
    def get_error(self) -> Exception:
        return self._error_queue.get()

    def put_error(self, e: Exception):
        self._error_queue.put(e)
    
    def submit(self, func, /, args=(), kwargs=None, daemon=False):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                self._error_queue.put(e)
        t = threading.Thread(target=wrapper, args=args, kwargs=kwargs, daemon=daemon)
        t.start()
        self._threads.append(t)
