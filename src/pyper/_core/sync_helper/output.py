from __future__ import annotations

from typing import TYPE_CHECKING
import queue

from .stage import Producer, ProducerConsumer
from ..util.sentinel import StopSentinel
from ..util.thread_pool import ThreadPool

if TYPE_CHECKING:
    from ..pipeline import Pipeline


class PipelineOutput:
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline

    def _get_q_out(self, tp: ThreadPool, *args, **kwargs) -> queue.Queue:
        """Feed forward each stage to the next, returning the output queue of the final stage."""
        q_out = None
        for task, next_task in zip(self.pipeline.tasks, self.pipeline.tasks[1:] + [None]):
            n_consumers = 1 if next_task is None else next_task.concurrency
            if q_out is None:
                stage = Producer(task=self.pipeline.tasks[0], tp=tp, n_consumers=n_consumers)
                stage.start(*args, **kwargs)
            else:
                stage = ProducerConsumer(q_in=q_out, task=task, tp=tp, n_consumers=n_consumers)
                stage.start()
            q_out = stage.q_out

        return q_out
    
    def __call__(self, *args, **kwargs):
        """Call the pipeline, taking the inputs to the first task, and returning the output from the last task."""
        with ThreadPool() as tp:
            q_out = self._get_q_out(tp, *args, **kwargs)
            while True:
                tp.raise_error_if_exists()
                try:
                    # Use the timeout strategy for unblocking main thread without busy waiting
                    if (data := q_out.get(timeout=0.1)) is StopSentinel:
                        tp.raise_error_if_exists()
                        break
                    yield data
                except queue.Empty:
                    continue
