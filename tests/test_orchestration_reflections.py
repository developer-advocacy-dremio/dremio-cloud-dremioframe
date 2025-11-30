import pytest
from unittest.mock import MagicMock
from dremioframe.orchestration import RefreshReflectionTask

def test_refresh_reflection_task_sql():
    mock_client = MagicMock()
    task = RefreshReflectionTask("refresh", mock_client, "my.dataset")
    assert task.sql == "ALTER DATASET my.dataset REFRESH REFLECTIONS"
