"""
PipelineEngine - A lightweight, robust, thread-safe task orchestration engine.
"""

from .declarative_pipeline import (
    PipelineElement,
    PipelineEngine,
    PipelinePlan,
    TaskState
)

__version__ = "0.1.0"

__all__ = [
    "PipelineEngine",
    "PipelineElement",
    "PipelinePlan",
    "TaskState",
    "__version__",
]