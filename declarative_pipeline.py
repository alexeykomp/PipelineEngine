"""
declarative_pipeline.py

A lightweight, robust, thread-safe task orchestration engine.
Executes multi-step pipelines over arbitrary data elements using a thread pool,
supporting step-level concurrency throttling, progress reporting, cancellation,
and graceful idle auto-stopping.
"""

from __future__ import annotations

import dataclasses
import logging
import threading
import traceback
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from enum import Enum, auto
from queue import Empty, Queue
from typing import Any

logger = logging.getLogger(__name__)


class TaskState(Enum):
    """Lifecycle states of an element within the pipeline."""
    WAITING = auto()
    RUNNING = auto()
    FINISHED = auto()
    FAILED = auto()
    CANCELED = auto()


@dataclasses.dataclass
class StepReport:
    """Internal completion message passed from worker to dispatcher."""
    element: PipelineElement
    step_func: Callable[..., bool]
    success: bool
    exception: Exception | None = None


class PipelineElement:
    """
    Base class for any item processed through the pipeline.
    Subclass or instantiate with a unique identifier.
    """

    def __init__(self, identifier: str | int):
        self.identifier = identifier
        self.state: TaskState = TaskState.WAITING
        self.status_message: str = "Waiting"
        self.current_step: Callable[..., Any] | None = None
        self.step_pointer: int = 0
        self.progress: float = 0.0

        self._abort_event = threading.Event()
        self._progress_reporter: Callable[[float], None] | None = None

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PipelineElement):
            return self.identifier == other.identifier
        return False

    def __hash__(self) -> int:
        return hash(self.identifier)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} id={self.identifier} state={self.state.name}>"

    def request_abort(self) -> None:
        """Signal that this element should stop further processing."""
        self._abort_event.set()
        self.status_message = "Abort requested"

    @property
    def is_aborted(self) -> bool:
        return self._abort_event.is_set()

    def set_progress(self, value: float) -> None:
        """
        Updates in-step progress (0.0 to 1.0) and notifies the engine.
        Throttled to avoid overwhelming UI event loops on rapid updates.
        """
        val = max(0.0, min(1.0, float(value)))
        if abs(val - self.progress) >= 0.01 or val in (0.0, 1.0):
            self.progress = val
            if self._progress_reporter:
                self._progress_reporter(val)


@dataclasses.dataclass
class PipelinePlan:
    """
    Defines the sequence of operations and optional concurrency constraints.

    :param sequence: Ordered list of step callables. Signature: `step_func(element) -> bool`
    :param step_limits: Max concurrent workers per step, e.g. `{write_to_db: 1, upload_to_nas: 2}`
    """
    sequence: list[Callable[..., bool]]
    step_limits: dict[Callable[..., bool], int] = dataclasses.field(default_factory=dict)


class PipelineEngine:
    """
    Multithreaded execution engine for processing batches of elements
    through declarative sequential steps with step-level concurrency throttling.
    """

    def __init__(self, max_workers: int = 8, auto_stop: bool = False):
        self.max_workers = max_workers
        self.auto_stop = auto_stop

        self._executor: ThreadPoolExecutor | None = None
        self._completion_queue: Queue[StepReport] = Queue()

        self._active_elements: list[PipelineElement] = []
        self._element_plans: dict[PipelineElement, PipelinePlan] = {}

        self._step_active_counts: dict[Callable[..., bool], int] = defaultdict(int)
        self._step_waiting_queues: dict[Callable[..., bool], list[PipelineElement]] = defaultdict(list)

        self._lock = threading.Lock()
        self._dispatcher_stop_event = threading.Event()
        self._dispatcher_thread: threading.Thread | None = None

        # Clean Lifecycle Callbacks
        self.on_step_started: Callable[[PipelineElement, Callable], None] | None = None
        self.on_step_progress: Callable[[PipelineElement, Callable, float], None] | None = None
        self.on_step_finished: Callable[[PipelineElement, Callable, bool], None] | None = None
        self.on_element_finished: Callable[[PipelineElement], None] | None = None
        self.on_element_failed: Callable[[PipelineElement, Exception | None], None] | None = None
        self.on_element_canceled: Callable[[PipelineElement], None] | None = None
        self.on_pipeline_idle: Callable[[], None] | None = None

    def start(self) -> None:
        """Starts or revives the executor and dispatcher background thread."""
        with self._lock:
            if self._executor is None or getattr(self._executor, '_shutdown', False):
                self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
            if self._dispatcher_thread is None or not self._dispatcher_thread.is_alive():
                self._dispatcher_stop_event.clear()
                self._dispatcher_thread = threading.Thread(
                    target=self._dispatcher_loop, daemon=True, name="PipelineDispatcher"
                )
                self._dispatcher_thread.start()

    def shutdown(self, wait: bool = True) -> None:
        """Gracefully shuts down the dispatcher and thread pool from an external thread."""
        self._dispatcher_stop_event.set()
        with self._lock:
            if self._executor and not getattr(self._executor, '_shutdown', False):
                self._executor.shutdown(wait=wait)
        if wait and self._dispatcher_thread and self._dispatcher_thread.is_alive():
            # Never join if called from within the dispatcher thread itself!
            if threading.current_thread() is not self._dispatcher_thread:
                self._dispatcher_thread.join(timeout=2.0)

    def submit(self, elements: list[PipelineElement], plan: PipelinePlan) -> None:
        """Submits a batch of elements to be processed against a pipeline plan."""
        if not elements:
            return

        new_items: list[PipelineElement] = []
        with self._lock:
            for el in elements:
                if el not in self._active_elements:
                    el.state = TaskState.WAITING
                    el.step_pointer = 0
                    el._abort_event.clear()
                    el.progress = 0.0
                    self._active_elements.append(el)
                    self._element_plans[el] = plan
                    new_items.append(el)

        self.start()

        for el in new_items:
            self._schedule_next_step(el)

    def stop_all(self) -> None:
        """Signals cancellation to all currently tracked elements."""
        with self._lock:
            for el in self._active_elements:
                el.request_abort()

    def _schedule_next_step(self, el: PipelineElement) -> None:
        """Evaluates whether the element can advance or needs to wait for a step slot."""
        with self._lock:
            plan = self._element_plans.get(el)
            if not plan:
                return

            if el.is_aborted:
                self._handle_element_termination_locked(el, TaskState.CANCELED)
                return

            if el.step_pointer >= len(plan.sequence):
                self._handle_element_termination_locked(el, TaskState.FINISHED)
                return

            current_step = plan.sequence[el.step_pointer]
            el.current_step = current_step
            el.status_message = current_step.__name__
            el.state = TaskState.RUNNING

            limit = plan.step_limits.get(current_step)
            if limit is not None and self._step_active_counts[current_step] >= limit:
                self._step_waiting_queues[current_step].append(el)
                return

            if limit is not None:
                self._step_active_counts[current_step] += 1

        self._dispatch_to_executor(el, current_step, plan)

    def _dispatch_to_executor(self, el: PipelineElement, step: Callable[..., bool], plan: PipelinePlan) -> None:
        """Executes on_step_started synchronously, then sends work to thread pool."""
        el.progress = 0.0

        def report_progress(val: float) -> None:
            if self.on_step_progress:
                try:
                    self.on_step_progress(el, step, val)
                except Exception:
                    logger.error("Error in on_step_progress callback: %s", traceback.format_exc())

        el._progress_reporter = report_progress

        if self.on_step_started:
            try:
                self.on_step_started(el, step)
            except Exception:
                logger.error("Error in on_step_started callback: %s", traceback.format_exc())

        def worker_task() -> None:
            if el.is_aborted:
                self._completion_queue.put(StepReport(el, step, success=False))
                return

            try:
                res = step(el)
                self._completion_queue.put(StepReport(el, step, success=bool(res)))
            except Exception as exc:
                logger.error("Exception in step %s on %s: %s", step.__name__, el, traceback.format_exc())
                self._completion_queue.put(StepReport(el, step, success=False, exception=exc))

        with self._lock:
            if self._executor and not getattr(self._executor, '_shutdown', False):
                self._executor.submit(worker_task)

    def _dispatcher_loop(self) -> None:
        """Listens for completed steps and orchestrates pipeline progression."""
        while not self._dispatcher_stop_event.is_set():
            try:
                report: StepReport = self._completion_queue.get(timeout=0.1)
            except Empty:
                report = None

            if report:
                el = report.element
                prev_step = report.step_func
                was_success = report.success

                with self._lock:
                    plan = self._element_plans.get(el)

                if self.on_step_finished:
                    try:
                        self.on_step_finished(el, prev_step, was_success)
                    except Exception:
                        logger.error("Error in on_step_finished callback: %s", traceback.format_exc())

                # Release step concurrency slot and pull next queued item
                next_waiting_el: PipelineElement | None = None
                with self._lock:
                    limit = plan.step_limits.get(prev_step) if plan else None
                    if limit is not None:
                        waiting = self._step_waiting_queues[prev_step]
                        while waiting:
                            candidate = waiting.pop(0)
                            if not candidate.is_aborted:
                                next_waiting_el = candidate
                                break
                            else:
                                self._handle_element_termination_locked(candidate, TaskState.CANCELED)

                        if next_waiting_el is None:
                            self._step_active_counts[prev_step] = max(0, self._step_active_counts[prev_step] - 1)

                if next_waiting_el and plan:
                    self._dispatch_to_executor(next_waiting_el, prev_step, plan)

                # Advance or terminate element
                if el.is_aborted:
                    with self._lock:
                        self._handle_element_termination_locked(el, TaskState.CANCELED)
                elif not was_success:
                    el.status_message = f"Failed at {prev_step.__name__}"
                    with self._lock:
                        self._handle_element_termination_locked(el, TaskState.FAILED, exc=report.exception)
                else:
                    el.step_pointer += 1
                    self._schedule_next_step(el)

                self._completion_queue.task_done()

            # Check idle state
            with self._lock:
                is_idle = len(self._active_elements) == 0

            if is_idle:
                if self.on_pipeline_idle:
                    try:
                        self.on_pipeline_idle()
                    except Exception:
                        logger.error("Error in on_pipeline_idle callback: %s", traceback.format_exc())

                if self.auto_stop:
                    # Graceful internal auto-shutdown
                    with self._lock:
                        if self._executor and not getattr(self._executor, '_shutdown', False):
                            self._executor.shutdown(wait=False)
                    # Natural thread exit
                    return

    def _handle_element_termination_locked(
            self, el: PipelineElement, state: TaskState, exc: Exception | None = None
    ) -> None:
        """Internal helper to remove element and trigger lifecycle callbacks."""
        el.state = state
        if el in self._active_elements:
            self._active_elements.remove(el)
        self._element_plans.pop(el, None)

        if state == TaskState.FINISHED:
            el.status_message = "Completed"
            cb = self.on_element_finished
        elif state == TaskState.FAILED:
            cb = self.on_element_failed
        elif state == TaskState.CANCELED:
            el.status_message = "Canceled"
            cb = self.on_element_canceled
        else:
            cb = None

        if cb:
            try:
                if state == TaskState.FAILED:
                    cb(el, exc)
                else:
                    cb(el)
            except Exception:
                logger.error("Error in element lifecycle callback: %s", traceback.format_exc())