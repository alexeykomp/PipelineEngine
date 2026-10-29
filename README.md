# Declarative Pipeline Engine

A lightweight, pure-Python orchestration engine designed for executing multi-step, sequential pipelines across batches of data elements with fine-grained concurrency control.

## Key Features
* **Zero External Dependencies:** Built entirely with Python's standard library (`threading`, `queue`, `concurrent.futures`).
* **Step-Level Concurrency Throttling:** Restrict specific steps (e.g., database writes, NAS uploads) to a fixed number of threads without causing thread-pool starvation.
* **Non-Blocking Push Dispatcher:** Driven by event completion queues rather than CPU-polling timers.
* **Built-in Cancellation & Progress Throttling:** Instant task cancellation via `threading.Event` and built-in throttling for progress updates.
* **Auto-Stopping:** Automatically shuts down background threads and worker pools upon going idle.

---

## Architectural Constraints & Trade-Offs

When considering this engine for your architecture, keep the following design constraints in mind:

### 1. UI Thread Confinement
* **Constraint:** Engine callbacks (`on_step_started`, `on_step_progress`, etc.) execute from the background `PipelineDispatcher` thread, not the UI main thread.
* **Trade-off:** Directly modifying visual UI widgets (in Qt, Tkinter, etc.) from these callbacks violates UI-thread confinement rules.
* **Design Guideline:** Always redirect callback notifications to the main thread (e.g., via `PySide6.QtCore.Signal` with `Qt.QueuedConnection` or an event queue).

### 2. Step Independence
* **Constraint:** Steps are represented as independent callable functions `step(element) -> bool`.
* **Design Guideline:** Steps should communicate with each other exclusively by modifying properties on the passed `PipelineElement`. Steps must avoid relying on global mutable state to remain thread-safe.

### 3. Python GIL & Concurrency Model
* **Constraint:** The engine utilizes a `ThreadPoolExecutor`. In standard CPython, worker threads are bound by the Global Interpreter Lock (GIL).
* **Trade-off:**
    * **Excellent for:** I/O-bound tasks (file copying, database queries, HTTP requests, subprocess execution) and C-extension operations (Pillow image filters, OpenCV, NumPy).
    * **Limited for:** Heavy pure-Python CPU calculations. If tasks require raw CPU loops, consider running those tasks inside `multiprocessing` or calling compiled C/Rust extensions.

### 4. In-Memory State & Single-Node Scope
* **Constraint:** All task states and queues reside entirely in memory.
* **Trade-off:** If the host process crashes or the machine loses power, in-flight pipeline states are lost.
* **Design Guideline:** This engine is designed for local desktop tools, ETL scripts, and node-level pipelines. For distributed fault-tolerant processing across multiple servers, consider distributed systems like Celery, Temporal, or Airflow. Individual steps should be implemented as **idempotent** operations.
