import pytest
import os
import time
from dremioframe.orchestration.backend import PostgresBackend, MySQLBackend, PipelineRun

def get_sample_run():
    return PipelineRun(
        pipeline_name="test_pipeline",
        run_id="run_backend_test",
        start_time=time.time(),
        status="RUNNING",
        tasks={"task1": "PENDING"}
    )

@pytest.mark.external_backend
@pytest.mark.skipif(not os.environ.get("DREMIOFRAME_PG_DSN"), 
                    reason="Postgres DSN not found")
def test_postgres_backend_live():
    backend = PostgresBackend(dsn=os.environ.get("DREMIOFRAME_PG_DSN"))
    run = get_sample_run()
    backend.save_run(run)
    
    loaded = backend.get_run(run.run_id)
    assert loaded.pipeline_name == "test_pipeline"
    
    backend.update_task_status(run.run_id, "task1", "SUCCESS")
    loaded = backend.get_run(run.run_id)
    assert loaded.tasks["task1"] == "SUCCESS"

@pytest.mark.external_backend
@pytest.mark.skipif(not os.environ.get("DREMIOFRAME_MYSQL_USER"), 
                    reason="MySQL credentials not found")
def test_mysql_backend_live():
    config = {
        "user": os.environ.get("DREMIOFRAME_MYSQL_USER"),
        "password": os.environ.get("DREMIOFRAME_MYSQL_PASSWORD"),
        "host": os.environ.get("DREMIOFRAME_MYSQL_HOST", "localhost"),
        "database": os.environ.get("DREMIOFRAME_MYSQL_DB"),
        "port": int(os.environ.get("DREMIOFRAME_MYSQL_PORT", 3306))
    }
    backend = MySQLBackend(config=config)
    run = get_sample_run()
    backend.save_run(run)
    
    loaded = backend.get_run(run.run_id)
    assert loaded.pipeline_name == "test_pipeline"
    
    backend.update_task_status(run.run_id, "task1", "SUCCESS")
    loaded = backend.get_run(run.run_id)
    assert loaded.tasks["task1"] == "SUCCESS"
