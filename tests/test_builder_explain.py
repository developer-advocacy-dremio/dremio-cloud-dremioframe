import pytest
import pandas as pd
from unittest.mock import MagicMock
from dremioframe.builder import DremioBuilder

def test_explain(dremio_client):
    builder = dremio_client.table("my_table")
    
    # Mock _execute_flight
    mock_df = pd.DataFrame({"text": ["Plan text"]})
    builder._execute_flight = MagicMock(return_value=mock_df)
    
    plan = builder.filter("id > 5").explain()
    
    # Verify SQL
    expected_sql = "EXPLAIN PLAN FOR SELECT * FROM my_table WHERE id > 5"
    builder._execute_flight.assert_called_with(expected_sql, "pandas")
    
    # Verify result
    assert plan == "Plan text"

def test_explain_empty(dremio_client):
    builder = dremio_client.table("my_table")
    
    # Mock empty result
    builder._execute_flight = MagicMock(return_value=pd.DataFrame())
    
    plan = builder.explain()
    
    assert plan == "No plan returned."
