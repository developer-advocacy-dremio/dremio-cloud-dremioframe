import pytest
import pandas as pd
from unittest.mock import MagicMock
from dremioframe.client import DremioClient
from dremioframe.orchestration import DremioQueryTask

def test_dremio_query_task_success():
    mock_client = MagicMock(spec=DremioClient)
    # Mock client.execute
    mock_client.execute.return_value = pd.DataFrame({"col1": [1, 2]})
    
    task = DremioQueryTask("test_task", mock_client, "SELECT * FROM table")
    result = task.run({})
    
    assert result is not None
    mock_client.execute.assert_called_once_with("SELECT * FROM table")
    assert task.status == "SUCCESS"

def test_dremio_query_task_failure():
    mock_client = MagicMock(spec=DremioClient)
    # Mock execute to raise exception
    mock_client.execute.side_effect = Exception("Query failed")
    
    task = DremioQueryTask("test_task", mock_client, "SELECT * FROM table")
    
    with pytest.raises(Exception, match="Query failed"):
        task.run({})
        
    assert task.status == "FAILED" # Corrected from "assert task.status == "FAILED" in str(excinfo.value)"

def test_dremio_query_task_kill():
    mock_client = MagicMock()
    task = DremioQueryTask("test_kill", mock_client, "SELECT 1")
    task.job_id = "job_kill"
    
    task.on_kill()
    
    mock_client.api.post.assert_called_with("job/job_kill/cancel", json={})
