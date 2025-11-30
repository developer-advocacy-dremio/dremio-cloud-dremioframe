from dremioframe.client import DremioClient
from dremioframe.orchestration import Task, Pipeline, DataQualityTask, task, schedule_pipeline
import time
import random

# Mock client for demo purposes if no creds
try:
    client = DremioClient()
except:
    print("No Dremio credentials found, using mock mode for demo.")
    client = None

@task(name="create_source", retries=2)
def create_source():
    print("Creating source...")
    # Simulate occasional failure
    if random.random() < 0.3:
        raise ValueError("Random network error")
    return "source_created"

@task(name="create_table")
def create_table(context=None):
    # Access upstream result
    source_status = context.get("create_source")
    print(f"Creating table (Source status: {source_status})...")
    time.sleep(1) # Simulate work
    return "table_created"

@task(name="check_quality")
def check_data_quality(context=None):
    print("Checking data quality...")
    return True

@task(name="publish", trigger_rule="all_success")
def publish_dataset(context=None):
    print("Publishing dataset...")
    return "published"

@task(name="notify_failure", trigger_rule="one_failed")
def notify_failure(context=None):
    print("!!! ALERT: Pipeline failed somewhere upstream !!!")
    return "alert_sent"

@task(name="cleanup", trigger_rule="all_done")
def cleanup(context=None):
    print("Cleaning up resources (runs regardless of success/failure)...")
    return "cleaned"

# Define Dependencies
t_source = create_source()
t_table = create_table()
t_quality = check_data_quality()
t_publish = publish_dataset()
t_fail = notify_failure()
t_clean = cleanup()

# Main Flow
t_source.set_downstream(t_table)
t_table.set_downstream(t_quality)
t_quality.set_downstream(t_publish)

# Failure Branch (attached to all critical tasks)
t_source.set_downstream(t_fail)
t_table.set_downstream(t_fail)
t_quality.set_downstream(t_fail)

# Cleanup (runs after publish or failure)
t_publish.set_downstream(t_clean)
t_fail.set_downstream(t_clean)

# Create Pipeline with Parallel Execution
pipeline = Pipeline("etl_pipeline", max_workers=2)
pipeline.add_task(t_source).add_task(t_table).add_task(t_quality).add_task(t_publish).add_task(t_fail).add_task(t_clean)


# Run
if __name__ == "__main__":
    print("--- Running Pipeline ---")
    results = pipeline.run()
    print("Results:", results)
    
    print("\n--- Visualizing ---")
    print(pipeline.visualize())

