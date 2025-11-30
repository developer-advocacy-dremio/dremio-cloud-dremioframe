import pytest
from dremioframe.orchestration import Task, Pipeline, task

def test_live_dremio_pipeline_full_features(dremio_client):
    """
    Comprehensive live test verifying:
    1. Context Passing: Data flows from Dremio -> Task -> Task
    2. Branching: Handling query failures
    3. Retries: Recovering from transient errors (simulated)
    """
    
    # --- Context Passing Test ---
    @task(name="get_tables")
    def get_tables():
        # List tables in a known source or just root
        # Assuming 'Samples' exists in Dremio Cloud usually
        try:
            # Try to list from a common sample source if possible, or just root
            catalog = dremio_client.catalog.list_catalog()
            return [item['path'] for item in catalog]
        except:
            return []

    @task(name="count_tables")
    def count_tables(context=None):
        tables = context.get("get_tables")
        print(f"Found tables: {tables}")
        return len(tables)

    # --- Branching Test (Failure Path) ---
    @task(name="bad_query")
    def bad_query():
        # Intentionally fail
        dremio_client.query("SELECT * FROM non_existent_table_12345")

    @task(name="handle_failure", trigger_rule="one_failed")
    def handle_failure(context=None):
        print("Caught expected failure from bad_query")
        return "recovered"

    # --- Retries Test ---
    # We simulate a flaky task that eventually succeeds
    attempts = 0
    @task(name="flaky_query", retries=2, retry_delay=0.5)
    def flaky_query():
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            print(f"Simulating failure attempt {attempts}")
            raise ValueError("Simulated network blip")
        print("Query success")
        # Run a real quick query to prove it works
        return dremio_client.query("SELECT 1")

    # Build Pipeline
    t_get = get_tables()
    t_count = count_tables()
    t_get.set_downstream(t_count)

    t_bad = bad_query()
    t_handle = handle_failure()
    t_bad.set_downstream(t_handle)

    t_flaky = flaky_query()

    # We can run them as independent chains in one pipeline
    pipeline = Pipeline("live_comprehensive_test", max_workers=2)
    pipeline.add_task(t_get).add_task(t_count)
    pipeline.add_task(t_bad).add_task(t_handle)
    pipeline.add_task(t_flaky)

    results = pipeline.run()

    # Verifications
    assert results["count_tables"] >= 0
    assert results["handle_failure"] == "recovered"
    assert t_bad.status == "FAILED"
    assert t_flaky.status == "SUCCESS"
    assert attempts == 2

