"""
tests/test_pipeline.py
Unit tests for Declarative Pipeline Engine.
"""

import threading
import time
import unittest

from declarative_pipeline import (
    PipelineElement,
    PipelineEngine,
    PipelinePlan,
    TaskState,
)


class TestPipelineEngine(unittest.TestCase):

    def test_happy_path(self):
        """Verify sequential step progression across elements."""
        trace = []
        lock = threading.Lock()

        def step_1(el):
            with lock:
                trace.append((el.identifier, 1))
            return True

        def step_2(el):
            with lock:
                trace.append((el.identifier, 2))
            return True

        engine = PipelineEngine(max_workers=4, auto_stop=True)
        items = [PipelineElement("A"), PipelineElement("B")]
        plan = PipelinePlan(sequence=[step_1, step_2])

        idle_event = threading.Event()
        engine.on_pipeline_idle = lambda: idle_event.set()

        engine.submit(items, plan)
        self.assertTrue(idle_event.wait(timeout=3.0))

        self.assertEqual(items[0].state, TaskState.FINISHED)
        self.assertEqual(items[1].state, TaskState.FINISHED)
        self.assertIn(("A", 1), trace)
        self.assertIn(("A", 2), trace)

    def test_auto_stop_and_reentrancy(self):
        """Verify the engine shuts down on idle and revives when new items arrive."""
        engine = PipelineEngine(max_workers=2, auto_stop=True)
        plan = PipelinePlan(sequence=[lambda el: True])

        idle_event = threading.Event()
        engine.on_pipeline_idle = lambda: idle_event.set()

        # Batch 1
        items1 = [PipelineElement(1)]
        engine.submit(items1, plan)
        self.assertTrue(idle_event.wait(timeout=2.0))
        time.sleep(0.05)
        # Dispatcher thread should have exited naturally
        self.assertFalse(engine._dispatcher_thread.is_alive())

        # Batch 2: submit again should cleanly restart the engine
        idle_event.clear()
        items2 = [PipelineElement(2)]
        engine.submit(items2, plan)
        self.assertTrue(idle_event.wait(timeout=2.0))
        self.assertEqual(items2[0].state, TaskState.FINISHED)

    def test_step_progress_reporting_and_throttling(self):
        """Verify set_progress emits updates and suppresses sub-1% jitter."""
        progress_records = []

        def progressive_step(el):
            # 0.005 should be throttled out (less than 1% delta)
            for p in [0.0, 0.005, 0.25, 0.50, 0.75, 1.0]:
                el.set_progress(p)
            return True

        engine = PipelineEngine(max_workers=2, auto_stop=True)
        plan = PipelinePlan(sequence=[progressive_step])
        item = PipelineElement("progress_item")

        engine.on_step_progress = lambda el, step, val: progress_records.append(val)
        idle_event = threading.Event()
        engine.on_pipeline_idle = lambda: idle_event.set()

        engine.submit([item], plan)
        self.assertTrue(idle_event.wait(timeout=2.0))

        self.assertEqual(progress_records, [0.0, 0.25, 0.50, 0.75, 1.0])

    def test_cancellation_stops_further_steps(self):
        """Verify request_abort stops an element before subsequent steps run."""
        step2_started = threading.Event()
        step2_hold = threading.Event()
        step3_called = False

        def step_1(el):
            return True

        def step_2(el):
            step2_started.set()
            step2_hold.wait(timeout=2.0)
            return True

        def step_3(el):
            nonlocal step3_called
            step3_called = True
            return True

        engine = PipelineEngine(max_workers=4, auto_stop=True)
        plan = PipelinePlan(sequence=[step_1, step_2, step_3])
        item = PipelineElement("cancel_target")

        idle_event = threading.Event()
        engine.on_pipeline_idle = lambda: idle_event.set()

        engine.submit([item], plan)
        self.assertTrue(step2_started.wait(timeout=2.0))

        # Abort while step 2 is running
        item.request_abort()
        step2_hold.set()

        self.assertTrue(idle_event.wait(timeout=3.0))
        self.assertEqual(item.state, TaskState.CANCELED)
        self.assertFalse(step3_called, "Step 3 must not execute after abort")

    def test_step_concurrency_throttling(self):
        """Verify that step_limits prevents worker overload on specific steps."""
        active_count = 0
        max_active = 0
        conc_lock = threading.Lock()

        def heavy_step(el):
            nonlocal active_count, max_active
            with conc_lock:
                active_count += 1
                if active_count > max_active:
                    max_active = active_count
            time.sleep(0.05)
            with conc_lock:
                active_count -= 1
            return True

        engine = PipelineEngine(max_workers=8, auto_stop=True)
        # Limit heavy_step to 2 concurrent workers even though pool size is 8
        plan = PipelinePlan(
            sequence=[heavy_step],
            step_limits={heavy_step: 2}
        )
        items = [PipelineElement(i) for i in range(8)]

        idle_event = threading.Event()
        engine.on_pipeline_idle = lambda: idle_event.set()

        engine.submit(items, plan)
        self.assertTrue(idle_event.wait(timeout=5.0))

        self.assertLessEqual(max_active, 2, "Concurrency limit violated")
        self.assertTrue(all(el.state == TaskState.FINISHED for el in items))

    def test_long_running_mixed_workload(self):
        """Verify fast and slow steps interleave without deadlock."""
        def slow_step(el):
            time.sleep(0.08)
            return True

        def fast_step(el):
            return True

        engine = PipelineEngine(max_workers=4, auto_stop=True)
        plan = PipelinePlan(sequence=[slow_step, fast_step])
        items = [PipelineElement(i) for i in range(6)]

        idle_event = threading.Event()
        engine.on_pipeline_idle = lambda: idle_event.set()

        engine.submit(items, plan)
        self.assertTrue(idle_event.wait(timeout=4.0))
        self.assertTrue(all(el.state == TaskState.FINISHED for el in items))

    def test_step_failure_and_exception_isolation(self):
        """Verify failing steps stop that element without corrupting other elements."""
        def step_failing(el):
            if el.identifier == "bad":
                return False
            if el.identifier == "exploding":
                raise RuntimeError("Hardware disconnect")
            return True

        def step_final(el):
            return True

        engine = PipelineEngine(max_workers=4, auto_stop=True)
        plan = PipelinePlan(sequence=[step_failing, step_final])

        item_ok = PipelineElement("good")
        item_fail = PipelineElement("bad")
        item_err = PipelineElement("exploding")

        idle_event = threading.Event()
        engine.on_pipeline_idle = lambda: idle_event.set()

        engine.submit([item_ok, item_fail, item_err], plan)
        self.assertTrue(idle_event.wait(timeout=3.0))

        self.assertEqual(item_ok.state, TaskState.FINISHED)
        self.assertEqual(item_fail.state, TaskState.FAILED)
        self.assertEqual(item_err.state, TaskState.FAILED)


if __name__ == "__main__":
    unittest.main()