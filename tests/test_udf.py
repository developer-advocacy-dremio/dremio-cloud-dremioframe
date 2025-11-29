import pytest
from unittest.mock import MagicMock
from dremioframe.udf import UDFManager

@pytest.fixture
def mock_client():
    client = MagicMock()
    return client

def test_create_udf(mock_client):
    manager = UDFManager(mock_client)
    manager.create(
        name="my_func",
        args={"x": "INT", "y": "INT"},
        returns="INT",
        body="x + y"
    )
    
    mock_client.execute.assert_called_with("CREATE FUNCTION my_func (x INT, y INT) RETURNS INT RETURN x + y")

def test_create_udf_replace(mock_client):
    manager = UDFManager(mock_client)
    manager.create(
        name="my_func",
        args={"x": "INT"},
        returns="INT",
        body="x * 2",
        replace=True
    )
    
    mock_client.execute.assert_called_with("CREATE OR REPLACE FUNCTION my_func (x INT) RETURNS INT RETURN x * 2")

def test_drop_udf(mock_client):
    manager = UDFManager(mock_client)
    manager.drop("my_func")
    
    mock_client.execute.assert_called_with("DROP FUNCTION  my_func")

def test_drop_udf_if_exists(mock_client):
    manager = UDFManager(mock_client)
    manager.drop("my_func", if_exists=True)
    
    mock_client.execute.assert_called_with("DROP FUNCTION IF EXISTS my_func")

def test_list_udfs(mock_client):
    manager = UDFManager(mock_client)
    
    # Mock sql().collect().to_dict()
    mock_df = MagicMock()
    mock_df.to_dict.return_value = [{"ROUTINE_NAME": "my_func"}]
    mock_client.sql.return_value.collect.return_value = mock_df
    
    udfs = manager.list(pattern="my")
    
    assert len(udfs) == 1
    assert udfs[0]["ROUTINE_NAME"] == "my_func"
    mock_client.sql.assert_called_with("SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'FUNCTION' AND ROUTINE_NAME LIKE '%my%'")
