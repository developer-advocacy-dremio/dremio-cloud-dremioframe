import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import sys
from types import ModuleType

# Mock connectorx and sqlalchemy
mock_cx = ModuleType("connectorx")
mock_cx.read_sql = MagicMock() # Explicitly add attribute
sys.modules["connectorx"] = mock_cx

mock_sa = ModuleType("sqlalchemy")
mock_sa.create_engine = MagicMock() # Explicitly add attribute
sys.modules["sqlalchemy"] = mock_sa

from dremioframe.ingest.database import ingest_database

@pytest.fixture
def mock_client():
    client = MagicMock()
    client.table.return_value = MagicMock()
    return client

def test_ingest_database_connectorx(mock_client):
    # Mock connectorx.read_sql
    with patch("connectorx.read_sql") as mock_read:
        mock_read.return_value = pd.DataFrame({"id": [1], "val": ["a"]})
        
        ingest_database(
            mock_client, 
            "postgres://uri", 
            "SELECT * FROM t", 
            "target", 
            backend="connectorx"
        )
        
        mock_read.assert_called_with("postgres://uri", "SELECT * FROM t")
        
        # Verify write
        mock_client.table.assert_called_with("target")
        builder = mock_client.table.return_value
        builder.create.assert_called()
        args, kwargs = builder.create.call_args
        assert len(kwargs['data']) == 1

def test_ingest_database_sqlalchemy(mock_client):
    # Mock create_engine and pd.read_sql
    with patch("sqlalchemy.create_engine") as mock_engine:
        with patch("pandas.read_sql") as mock_pd_read:
            mock_pd_read.return_value = pd.DataFrame({"id": [1], "val": ["a"]})
            
            ingest_database(
                mock_client, 
                "sqlite:///", 
                "SELECT * FROM t", 
                "target", 
                backend="sqlalchemy",
                batch_size=None
            )
            
            mock_engine.assert_called_with("sqlite:///")
            mock_pd_read.assert_called()
            
            # Verify write
            mock_client.table.assert_called_with("target")
            builder = mock_client.table.return_value
            builder.create.assert_called()

def test_ingest_database_sqlalchemy_batching(mock_client):
    with patch("sqlalchemy.create_engine") as mock_engine:
        with patch("pandas.read_sql") as mock_pd_read:
            # Return iterator of chunks
            chunk1 = pd.DataFrame({"id": [1]})
            chunk2 = pd.DataFrame({"id": [2]})
            mock_pd_read.return_value = iter([chunk1, chunk2])
            
            ingest_database(
                mock_client, 
                "sqlite:///", 
                "SELECT * FROM t", 
                "target", 
                backend="sqlalchemy",
                batch_size=1
            )
            
            builder = mock_client.table.return_value
            # First chunk: create (replace)
            # Second chunk: insert (append)
            assert builder.create.call_count >= 1
            assert builder.insert.call_count >= 1
