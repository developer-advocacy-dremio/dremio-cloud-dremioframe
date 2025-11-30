import pytest
from unittest.mock import MagicMock
from dremioframe.orchestration.tasks.builder_task import DremioBuilderTask
from dremioframe.builder import DremioBuilder

def test_builder_task_create():
    mock_builder = MagicMock(spec=DremioBuilder)
    task = DremioBuilderTask("create_task", mock_builder, "create", "target_table")
    
    task.run({})
    
    mock_builder.create.assert_called_with("target_table")
    assert task.status == "SUCCESS"

def test_builder_task_merge():
    mock_builder = MagicMock(spec=DremioBuilder)
    options = {"on": "id", "matched_update": {"col": "val"}}
    task = DremioBuilderTask("merge_task", mock_builder, "merge", "target_table", options)
    
    task.run({})
    
    mock_builder.merge.assert_called_with("target_table", on="id", matched_update={"col": "val"})
    assert task.status == "SUCCESS"

def test_builder_task_invalid_command():
    mock_builder = MagicMock(spec=DremioBuilder)
    with pytest.raises(ValueError, match="Unsupported command"):
        DremioBuilderTask("bad_task", mock_builder, "unknown", "target")

def test_builder_task_missing_target():
    mock_builder = MagicMock(spec=DremioBuilder)
    with pytest.raises(ValueError, match="Target table is required"):
        DremioBuilderTask("bad_task", mock_builder, "create")
