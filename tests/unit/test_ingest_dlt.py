import pytest
from unittest.mock import MagicMock, patch
import pandas as pd

# Mock dlt library since it might not be installed in test env
import sys
from types import ModuleType

mock_dlt = ModuleType("dlt")
sys.modules["dlt"] = mock_dlt

from dremioframe.ingest.dlt import ingest_dlt

@pytest.fixture
def mock_client():
    client = MagicMock()
    client.table.return_value = MagicMock()
    return client

def test_ingest_dlt_single_resource(mock_client):
    # Mock a dlt resource (iterable of dicts)
    resource = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    # Add name attribute to list (simulating dlt resource)
    # But list doesn't allow attributes. Use a wrapper or mock.
    
    mock_res = MagicMock()
    del mock_res.resources # Ensure hasattr returns False
    mock_res.__iter__.return_value = iter(resource)
    mock_res.name = "my_resource"
    
    ingest_dlt(mock_client, mock_res, "target_table")
    
    # Verify create was called with dataframe
    mock_client.table.assert_called_with("target_table")
    builder = mock_client.table.return_value
    
    # Check create call
    assert builder.create.called
    args, kwargs = builder.create.call_args
    df = kwargs['data']
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.iloc[0]['val'] == 'a'

def test_ingest_dlt_multiple_resources(mock_client):
    # Mock source with resources dict
    res1 = MagicMock()
    res1.__iter__.return_value = iter([{"id": 1}])
    res1.name = "res1"
    
    res2 = MagicMock()
    res2.__iter__.return_value = iter([{"id": 2}])
    res2.name = "res2"
    
    source = MagicMock()
    source.resources = {"res1": res1, "res2": res2}
    
    ingest_dlt(mock_client, source, "base_table")
    
    # Should call table() for base_table_res1 and base_table_res2
    mock_client.table.assert_any_call("base_table_res1")
    mock_client.table.assert_any_call("base_table_res2")

def test_ingest_dlt_batching(mock_client):
    # 3 items, batch size 2 -> 2 batches
    resource = [{"id": 1}, {"id": 2}, {"id": 3}]
    mock_res = MagicMock()
    del mock_res.resources # Ensure hasattr returns False
    mock_res.__iter__.return_value = iter(resource)
    mock_res.name = "res"
    
    ingest_dlt(mock_client, mock_res, "target", batch_size=2)
    
    builder = mock_client.table.return_value
    
    # First batch: create (replace mode default)
    # Second batch: insert (append mode)
    assert builder.create.call_count >= 1
    assert builder.insert.call_count >= 1
    
    # Verify data in calls
    # create called with first 2
    create_args = builder.create.call_args_list[0]
    assert len(create_args[1]['data']) == 2
    
    # insert called with last 1
    insert_args = builder.insert.call_args_list[0]
    assert len(insert_args[1]['data']) == 1
