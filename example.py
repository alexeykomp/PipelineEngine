"""
example_usage.py

Demonstration of Declarative Pipeline Engine in a realistic e-commerce
digital asset ingestion workflow.
"""

import time
from declarative_pipeline import PipelineElement, PipelineEngine, PipelinePlan


class ProductAsset(PipelineElement):
    """Domain model representing a set of product photos to process."""
    def __init__(self, sku: str, file_count: int):
        super().__init__(identifier=sku)
        self.sku = sku
        self.file_count = file_count
        self.metadata: dict = {}


# --- Step Definitions ---

def read_and_validate_metadata(asset: ProductAsset) -> bool:
    """Step 1: Read and validate EXIF/Catalog info (Fast CPU)."""
    time.sleep(0.05)
    asset.metadata = {"sku": asset.sku, "status": "validated"}
    return True


def optimize_images(asset: ProductAsset) -> bool:
    """Step 2: Resize and optimize images with progress reporting (CPU bound)."""
    for i in range(asset.file_count):
        if asset.is_aborted:
            return False
        time.sleep(0.04)  # Simulate image resizing
        asset.set_progress((i + 1) / asset.file_count)
    return True


def upload_to_nas(asset: ProductAsset) -> bool:
    """Step 3: Transfer files to network storage (I/O bound)."""
    time.sleep(0.1)  # Simulate network transfer
    return True


def update_database(asset: ProductAsset) -> bool:
    """Step 4: Update catalog DB (Requires strictly 1 connection)."""
    time.sleep(0.02)
    return True


# --- Main Application Execution ---

def main():
    print("=== Starting E-Commerce Asset Pipeline ===")

    # Create engine with 8 worker threads and auto-stopping enabled
    engine = PipelineEngine(max_workers=8, auto_stop=True)

    # Define the execution plan:
    # Notice: upload_to_nas is limited to 2 concurrent threads to protect the network.
    # update_database is serialized to 1 thread to prevent write collisions.
    plan = PipelinePlan(
        sequence=[read_and_validate_metadata, optimize_images, upload_to_nas, update_database],
        step_limits={
            upload_to_nas: 2,
            update_database: 1
        }
    )

    # Attach Observer Callbacks
    engine.on_step_started = lambda el, step: print(f"[{el.identifier}] Started: {step.__name__}")
    engine.on_step_progress = lambda el, step, p: print(f"[{el.identifier}] {step.__name__} Progress: {p*100:.0f}%")
    engine.on_element_finished = lambda el: print(f"--> [{el.identifier}] COMPLETED ALL STEPS!")
    engine.on_element_failed = lambda el, exc: print(f"XX [{el.identifier}] FAILED: {exc}")
    engine.on_pipeline_idle = lambda: print("=== Pipeline is IDLE (All tasks completed) ===")

    # Prepare batch of 4 products
    batch = [
        ProductAsset("SKU-101", file_count=5),
        ProductAsset("SKU-102", file_count=3),
        ProductAsset("SKU-103", file_count=10),
        ProductAsset("SKU-104", file_count=2),
    ]

    # Submit batch
    engine.submit(batch, plan)

    # Wait for completion (engine will auto-stop its threads once done)
    if engine._dispatcher_thread:
        engine._dispatcher_thread.join()

    print("Execution finished cleanly. Engine shut down automatically.")


if __name__ == "__main__":
    main()