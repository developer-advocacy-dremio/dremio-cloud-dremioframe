import pytest
import os
import time
import pandas as pd
import pyarrow as pa
from unittest.mock import MagicMock, patch
from dremioframe.builder import DremioBuilder
from dremioframe.local_builder import LocalBuilder

@pytest.fixture
def mock_builder():
    builder = DremioBuilder(MagicMock(), "source.table")
    # Mock collect to return a simple Arrow Table
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    table = pa.Table.from_pandas(df)
    builder.collect = MagicMock(return_value=table)
    return builder

def test_cache_creation(mock_builder, tmp_path):
    cache_dir = tmp_path / "cache"
    local_builder = mock_builder.cache("test_cache", folder=str(cache_dir))
    
    assert os.path.exists(cache_dir / "test_cache.feather")
    assert isinstance(local_builder, LocalBuilder)
    assert mock_builder.collect.called

def test_cache_ttl_valid(mock_builder, tmp_path):
    cache_dir = tmp_path / "cache"
    os.makedirs(cache_dir)
    file_path = cache_dir / "test_ttl.feather"
    
    # Create a dummy file
    df = pd.DataFrame({"a": [1], "b": [2]})
    table = pa.Table.from_pandas(df)
    import pyarrow.feather as feather
    feather.write_feather(table, file_path)
    
    # Set mtime to now
    now = time.time()
    os.utime(file_path, (now, now))
    
    # Cache with TTL 60s (valid)
    local_builder = mock_builder.cache("test_ttl", ttl_seconds=60, folder=str(cache_dir))
    
    # Should NOT call collect (used cache)
    assert not mock_builder.collect.called
    assert isinstance(local_builder, LocalBuilder)

def test_cache_ttl_expired(mock_builder, tmp_path):
    cache_dir = tmp_path / "cache"
    os.makedirs(cache_dir)
    file_path = cache_dir / "test_expired.feather"
    
    # Create a dummy file
    df = pd.DataFrame({"a": [1], "b": [2]})
    table = pa.Table.from_pandas(df)
    import pyarrow.feather as feather
    feather.write_feather(table, file_path)
    
    # Set mtime to 2 hours ago
    past = time.time() - 7200
    os.utime(file_path, (past, past))
    
    # Cache with TTL 60s (expired)
    local_builder = mock_builder.cache("test_expired", ttl_seconds=60, folder=str(cache_dir))
    
    # Should call collect (refreshed cache)
    assert mock_builder.collect.called
    assert isinstance(local_builder, LocalBuilder)

def test_local_builder_query(tmp_path):
    # Create a feather file
    file_path = tmp_path / "data.feather"
    df = pd.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
    table = pa.Table.from_pandas(df)
    import pyarrow.feather as feather
    feather.write_feather(table, file_path)
    
    lb = LocalBuilder(str(file_path), "test")
    
    # Test SQL
    res = lb.sql("SELECT * FROM test WHERE a > 1").collect("pandas")
    assert len(res) == 2
    assert 1 not in res["a"].values
    
    # Test select/filter chaining (if implemented via SQL generation or similar)
    # Our LocalBuilder implementation uses sql() method mainly for filter/agg logic internally
    # Let's test filter
    res2 = lb.filter("a < 3").collect("pandas")
    # Should have a=2 (since previous sql call updated current_df? No, sql() updates current_df)
    # Wait, lb.sql() updates self.current_df.
    # So res was filtered to a > 1 (2, 3).
    # Then filter "a < 3" on that result -> 2.
    assert len(res2) == 1
    assert res2.iloc[0]["a"] == 2
