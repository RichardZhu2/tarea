from __future__ import annotations

from multiprocessing.managers import SyncManager
import threading


class MultiprocessingCounter:
    """Utility class to manage process based access to an integer."""
    def __init__(self, value: int, manager: SyncManager):
        self._manager_value = manager.Value('i', value)
        self._lock = manager.Lock()
    
    def increment(self):
        with self._lock:
            self._manager_value.value += 1
            return self._manager_value.value


class ThreadingCounter:
    """Utility class to manage thread based access to an integer."""
    def __init__(self, value: int):
        self._value = value
        self._lock = threading.Lock()
    
    def increment(self):
        with self._lock:
            self._value += 1
            return self._value
