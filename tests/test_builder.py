import pytest
import pandas as pd
from dremioframe.builder import DremioBuilder
import polars as pl

def test_compile_sql_simple(dremio_client):
    builder = dremio_client.table("Samples.samples.dremio.com.zips.json")
    sql = builder.select("city", "state").filter("state = 'MA'").limit(10)._compile_sql()
    expected = "SELECT city, state FROM Samples.samples.dremio.com.zips.json WHERE state = 'MA' LIMIT 10"
    assert sql == expected

def test_compile_sql_raw(dremio_client):
    builder = dremio_client.sql("SELECT * FROM foo")
    sql = builder.filter("id > 5")._compile_sql()
    expected = "SELECT * FROM (SELECT * FROM foo) AS sub WHERE id > 5"
    assert sql == expected

def test_mutate(dremio_client):
    builder = dremio_client.table("my_table")
    sql = builder.select("id").mutate(new_col="id * 2")._compile_sql()
    expected = "SELECT id, id * 2 AS new_col FROM my_table"
    assert sql == expected

def test_insert_dataframe(dremio_client):
    dremio_client.pat = "mock_pat"
    builder = dremio_client.table("dummy")
    builder._execute_dml = lambda sql: sql
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    sql = builder.insert("target_table", data=df)
    expected = "INSERT INTO target_table (id, name) VALUES (1, 'Alice'), (2, 'Bob')"
    assert sql == expected

def test_insert_dataframe_batched(dremio_client):
    dremio_client.pat = "mock_pat"
    builder = dremio_client.table("dummy")
    
    # Mock _execute_dml to collect SQLs
    sqls = []
    builder._execute_dml = lambda sql: sqls.append(sql)
    
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
    builder.insert("target_table", data=df, batch_size=2)
    
    assert len(sqls) == 2
    assert "VALUES (1, 'A'), (2, 'B')" in sqls[0]
    assert "VALUES (3, 'C')" in sqls[1]

def test_merge_sql(dremio_client):
    dremio_client.pat = "mock_pat"
    builder = dremio_client.table("source_table")
    builder._execute_dml = lambda sql: sql
    
    # Test merge with current builder as source
    sql = builder.select("id", "val").merge(
        target_table="target",
        on="id",
        matched_update={"val": "source.val"},
        not_matched_insert={"id": "source.id", "val": "source.val"}
    )
    
    expected_start = "MERGE INTO target USING (SELECT id, val FROM source_table) AS source ON (target.id = source.id)"
    expected_update = "WHEN MATCHED THEN UPDATE SET val = source.val"
    expected_insert = "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (source.id, source.val)"
    
    assert expected_start in sql
    assert expected_update in sql
    assert expected_insert in sql

def test_merge_dataframe(dremio_client):
    dremio_client.pat = "mock_pat"
    builder = dremio_client.table("dummy")
    builder._execute_dml = lambda sql: sql
    
    df = pd.DataFrame({"id": [1], "val": [100]})
    
    sql = builder.merge(
        target_table="target",
        on="id",
        matched_update={"val": "source.val"},
        data=df
    )
    
    # Check if source is VALUES clause
    assert "USING (VALUES (1, 100)) AS source(id, val)" in sql
    assert "ON (target.id = source.id)" in sql
    assert "WHEN MATCHED THEN UPDATE SET val = source.val" in sql

def test_quality_check(dremio_client):
    dremio_client.pat = "mock_pat"
    builder = dremio_client.table("dummy")
    
    # Mock _execute_flight to return 0 bad rows
    builder._execute_flight = lambda sql, lib: pl.DataFrame({"bad_rows": [0]})
    assert builder.quality.expect_not_null("id") == True

    # Mock _execute_flight to return 1 bad row (failure)
    builder._execute_flight = lambda sql, lib: pl.DataFrame({"bad_rows": [1]})
    with pytest.raises(ValueError, match="Data Quality Check Failed"):
        builder.quality.expect_not_null("id")

def test_custom_quality_check(dremio_client):
    dremio_client.pat = "mock_pat"
    builder = dremio_client.table("dummy")
    
    # Mock _execute_flight to return count=5
    builder._execute_flight = lambda sql, lib: pl.DataFrame({"cnt": [5]})
    
    # Expect count == 5 (Pass)
    assert builder.quality.expect_row_count("age > 18", 5, "eq") == True
    
    # Expect count > 0 (Pass)
    assert builder.quality.expect_row_count("age > 18", 0, "gt") == True
    
    # Expect count < 3 (Fail)
    with pytest.raises(ValueError, match="Custom Quality Check Failed"):
        builder.quality.expect_row_count("age > 18", 3, "lt")
