import pytest
import os
from dotenv import load_dotenv
from dremioframe.client import DremioClient
from dremioframe.orchestration import Pipeline, DremioQueryTask
from dremioframe.orchestration.backend import SQLiteBackend
from dremioframe.orchestration.executors import LocalExecutor

# Load env vars
load_dotenv()

@pytest.mark.skipif(not os.environ.get("DREMIO_PAT") or not os.environ.get("DREMIO_PROJECT_ID"), 
                    reason="Dremio credentials not found in environment")
def test_orchestration_live_dremio():
    # 1. Setup Client
    client = DremioClient(
        pat=os.environ.get("DREMIO_PAT"),
        project_id=os.environ.get("DREMIO_PROJECT_ID"),
        base_url="https://api.dremio.cloud"
    )
    
    # 2. Setup Orchestration
    # Use a temp db for testing
    db_path = "test_orchestration.db"
    if os.path.exists(db_path):
        os.remove(db_path)
        
    backend = SQLiteBackend(db_path)
    executor = LocalExecutor(backend=backend)
    pipeline = Pipeline("live_test_pipeline", backend=backend, executor=executor)
    
    # 3. Add Task
    # Simple query that should always succeed
    task = DremioQueryTask(
        name="test_select_1",
        client=client,
        sql="SELECT 1 as val"
    )
    pipeline.add_task(task)
    
    # 4. Run
    context = pipeline.run()
    
    # 5. Verify
    # DremioQueryTask returns the job_id (or result depending on implementation)
    # Let's check the backend state
    run = backend.list_runs(pipeline_name="live_test_pipeline", limit=1)[0]
    
    assert run.status == "SUCCESS"
    assert run.tasks["test_select_1"] == "SUCCESS"
    
    # Cleanup
    if os.path.exists(db_path):
        os.remove(db_path)
