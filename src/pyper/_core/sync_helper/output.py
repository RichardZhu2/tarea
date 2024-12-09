from __future__ import annotations

from typing import TYPE_CHECKING
import queue

from .stage import Producer, ProducerConsumer
from ..util.sentinel import StopSentinel
from ..util.worker_pool import ProcessPool, ThreadPool

if TYPE_CHECKING:
    from ..pipeline import Pipeline


class PipelineOutput:
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline

    def _get_q_out(self, tp: ThreadPool, pp: ProcessPool, *args, **kwargs) -> queue.Queue:
        """Feed forward each stage to the next, returning the output queue of the final stage."""
        q_out = None
        for task, next_task in zip(self.pipeline.tasks, self.pipeline.tasks[1:] + [None]):
            pool = pp if task.multiprocess else tp
            if q_out is None:
                stage = Producer(task=task, next_task=next_task, q_err=pool.error_queue, shutdown_event=pool.shutdown_event)
                stage.start(pool, *args, **kwargs)
            else:
                stage = ProducerConsumer(q_in=q_out, task=task, next_task=next_task, q_err=pool.error_queue, shutdown_event=pool.shutdown_event)
                stage.start(pool)
            q_out = stage.q_out

        return q_out
    
    def __call__(self, *args, **kwargs):
        """Iterate through the pipeline, taking the inputs to the first task, and yielding each output from the last task."""
        with ThreadPool() as tp, ProcessPool() as pp:
            q_out = self._get_q_out(tp, pp, *args, **kwargs)
            while True:
                try:
                    # Use the timeout strategy for unblocking main thread without busy waiting
                    if (data := q_out.get(timeout=0.1)) is StopSentinel:
                        tp.raise_error_if_exists()
                        pp.raise_error_if_exists()
                        break
                    yield data
                except queue.Empty:
                    tp.raise_error_if_exists()
                    pp.raise_error_if_exists()
                except (KeyboardInterrupt, SystemExit): # pragma: no cover
                    tp.shutdown_event.set()
                    pp.shutdown_event.set()
                    raise
