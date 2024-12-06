from __future__ import annotations

import threading
from typing import Any


class ThreadingValue:
    """Utility class to help manage thread based access to a value.
    
    The `get_lock` method mimics the API of `multiprocessing.Value`
    """
    def __init__(self, value: Any):
        self.value = value
        self._lock = threading.Lock()
    
    def get_lock(self):
        return self._lock
