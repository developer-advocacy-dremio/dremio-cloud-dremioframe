import pytest
import os
import time
from dotenv import load_dotenv
from dremioframe.client import DremioClient
from dremioframe.orchestration import Pipeline, DremioQueryTask, DremioBuilderTask
from dremioframe.orchestration.backend import InMemoryBackend
from dremioframe.orchestration.executors import LocalExecutor

load_dotenv()

@pytest.fixture
def integration_client():
    pat = os.environ.get("DREMIO_PAT")
    project_id = os.environ.get("DREMIO_PROJECT_ID")
    if not pat or not project_id:
        pytest.skip("Dremio credentials not found")
    
    return DremioClient(pat=pat, project_id=project_id)

@pytest.fixture
def test_space():
    space = os.environ.get("DREMIO_TEST_SPACE")
    if not space:
        pytest.skip("DREMIO_TEST_SPACE not set")
    return space

@pytest.mark.cloud
def test_etl_pipeline_sql(integration_client, test_space):
    """Test ETL pipeline using raw SQL tasks."""
    source_table = f'"{test_space}".source_data_sql'
    target_table = f'"{test_space}".target_data_sql'
    
    # Cleanup
    for tbl in [source_table, target_table]:
        try:
            integration_client.execute(f"DROP TABLE IF EXISTS {tbl}")
        except:
            pass

    # Create Source Data
    setup_sql = f"""
    CREATE TABLE {source_table} AS
    SELECT * FROM (VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)) AS t(id, name, val)
    """
    integration_client.execute(setup_sql)

    backend = InMemoryBackend()
    executor = LocalExecutor(backend)
    pipeline = Pipeline("sql_etl", backend=backend, executor=executor)
    
    # Task 1: Create Target Table (CTAS) from Source
    create_task = DremioQueryTask(
        name="create_table",
        client=integration_client,
        sql=f"""
        CREATE TABLE {target_table} AS
        SELECT id, name, val * 2 as val_doubled
        FROM {source_table}
        """
    )
    pipeline.add_task(create_task)
    
    context = pipeline.run()
    
    if "create_table" not in context:
        print(f"Context keys: {context.keys()}")
        
    assert context["create_table"] is not None
    
    # Verify
    df = integration_client.query(f"SELECT COUNT(*) as cnt FROM {target_table}")
    assert df.iloc[0, 0] == 3
    
    # Cleanup
    for tbl in [source_table, target_table]:
        try:
            integration_client.execute(f"DROP TABLE IF EXISTS {tbl}")
        except:
            pass

@pytest.mark.cloud
def test_etl_pipeline_builder(integration_client, test_space):
    """Test ETL pipeline using DremioBuilder tasks."""
    source_table = f'"{test_space}".source_data_builder'
    target_table = f'"{test_space}".target_data_builder'
    
    # Cleanup
    for tbl in [source_table, target_table]:
        try:
            integration_client.execute(f"DROP TABLE IF EXISTS {tbl}")
        except:
            pass

    # Create Source Data
    setup_sql = f"""
    CREATE TABLE {source_table} AS
    SELECT * FROM (VALUES (1, 'X', 100), (2, 'Y', 200)) AS t(id, name, val)
    """
    integration_client.execute(setup_sql)

    backend = InMemoryBackend()
    executor = LocalExecutor(backend)
    pipeline = Pipeline("builder_etl", backend=backend, executor=executor)
    
    # Define Builder
    builder = integration_client.table(source_table) \
        .select("id", "name", "val") \
        .mutate(val_plus_one="val + 1")
        
    # Task 1: Create Table using Builder
    create_task = DremioBuilderTask(
        name="create_table_builder",
        builder=builder,
        command="create",
        target=target_table
    )
    pipeline.add_task(create_task)
    
    pipeline.run()
    
    # Verify
    df = integration_client.query(f"SELECT COUNT(*) as cnt FROM {target_table}")
    assert df.iloc[0, 0] == 2
    
    # Cleanup
    for tbl in [source_table, target_table]:
        try:
            integration_client.execute(f"DROP TABLE IF EXISTS {tbl}")
        except:
            pass
