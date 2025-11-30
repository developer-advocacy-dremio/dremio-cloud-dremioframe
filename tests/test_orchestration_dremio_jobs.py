import pytest
from unittest.mock import MagicMock
from dremioframe.orchestration import DremioQueryTask

def test_dremio_query_task_success():
    mock_client = MagicMock()
    # Mock submit response
    mock_client.api.post.return_value = {"id": "job_123"}
    # Mock poll response (Running then Completed)
    mock_client.api.get.side_effect = [
        {"jobState": "RUNNING"},
        {"jobState": "COMPLETED"}
    ]
    
    task = DremioQueryTask("test_query", mock_client, "SELECT 1")
    result = task.run()
    
    assert result == "job_123"
    mock_client.api.post.assert_called_with("sql", json={"sql": "SELECT 1"})
    assert mock_client.api.get.call_count == 2

def test_dremio_query_task_failure():
    mock_client = MagicMock()
    mock_client.api.post.return_value = {"id": "job_fail"}
    mock_client.api.get.return_value = {"jobState": "FAILED"}
    
    task = DremioQueryTask("test_fail", mock_client, "SELECT 1")
    
    with pytest.raises(Exception) as excinfo:
        task.run()
    
    assert "failed with status: FAILED" in str(excinfo.value)

def test_dremio_query_task_kill():
    mock_client = MagicMock()
    task = DremioQueryTask("test_kill", mock_client, "SELECT 1")
    task.job_id = "job_kill"
    
    task.on_kill()
    
    mock_client.api.post.assert_called_with("job/job_kill/cancel", json={})
