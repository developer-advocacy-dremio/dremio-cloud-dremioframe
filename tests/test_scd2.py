import pytest
from dremioframe.client import DremioClient
from dremioframe.builder import DremioBuilder
from unittest.mock import MagicMock

def test_scd2_generation():
    client = MagicMock(spec=DremioClient)
    builder = DremioBuilder(client, "source_table")
    
    # Mock _execute_dml
    builder._execute_dml = MagicMock()
    
    builder.scd2(
        target_table="target_table",
        on=["id"],
        track_cols=["name", "status"]
    )
    
    # Check calls (should be 2)
    assert builder._execute_dml.call_count == 2
    
    # Check Close SQL
    close_call = builder._execute_dml.call_args_list[0][0][0]
    assert "UPDATE target_table" in close_call
    assert "SET valid_to = CURRENT_TIMESTAMP" in close_call
    assert "target_table.id = source.id" in close_call
    assert "target_table.name <> source.name" in close_call
    
    # Check Insert SQL
    insert_call = builder._execute_dml.call_args_list[1][0][0]
    assert "INSERT INTO target_table" in insert_call
    assert "SELECT source.*, CURRENT_TIMESTAMP, NULL" in insert_call
    assert "LEFT JOIN target_table" in insert_call
    assert "target_table.valid_to IS NULL" in insert_call
