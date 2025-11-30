import pytest
import os
from dremioframe.orchestration.backend import SQLiteBackend, PipelineRun, InMemoryBackend
from dremioframe.orchestration import Task, Pipeline
import time

def test_sqlite_backend_persistence():
    db_path = "test_orchestration.db"
    if os.path.exists(db_path):
        os.remove(db_path)
        
    backend = SQLiteBackend(db_path=db_path)
    
    # Create a run
    run = PipelineRun(
        pipeline_name="test_pipeline",
        run_id="run_1",
        start_time=time.time(),
        status="RUNNING",
        tasks={"t1": "PENDING"}
    )
    backend.save_run(run)
    
    # Verify retrieval
    loaded_run = backend.get_run("run_1")
    assert loaded_run is not None
    assert loaded_run.pipeline_name == "test_pipeline"
    assert loaded_run.status == "RUNNING"
    assert loaded_run.tasks["t1"] == "PENDING"
    
    # Update task status
    backend.update_task_status("run_1", "t1", "SUCCESS")
    loaded_run = backend.get_run("run_1")
    assert loaded_run.tasks["t1"] == "SUCCESS"
    
    # List runs
    runs = backend.list_runs()
    assert len(runs) == 1
    assert runs[0].run_id == "run_1"
    
    # Cleanup
    os.remove(db_path)

def test_pipeline_integration_with_backend():
    db_path = "test_pipeline_backend.db"
    if os.path.exists(db_path):
        os.remove(db_path)
        
    backend = SQLiteBackend(db_path=db_path)
    pipeline = Pipeline("backend_test_pipeline", backend=backend)
    
    t1 = Task("t1", lambda: "success")
    pipeline.add_task(t1)
    
    pipeline.run()
    
    # Verify run was saved
    runs = backend.list_runs("backend_test_pipeline")
    assert len(runs) == 1
    run = runs[0]
    assert run.status == "SUCCESS"
    assert run.tasks["t1"] == "SUCCESS"
    
    os.remove(db_path)
