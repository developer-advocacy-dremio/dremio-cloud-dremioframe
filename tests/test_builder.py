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
    expected = 'SELECT id, id * 2 AS "new_col" FROM my_table'
    assert sql == expected

def test_mutate_conflict(dremio_client):
    # Test conflict resolution: select("A").mutate(A="B+1") -> SELECT B+1 AS "A"
    builder = dremio_client.table("my_table")
    sql = builder.select("A").mutate(A="B+1")._compile_sql()
    expected = 'SELECT B+1 AS "A" FROM my_table'
    assert sql == expected

def test_mutate_disjoint(dremio_client):
    # Test disjoint: select("A").mutate(B="A+1") -> SELECT A, A+1 AS "B"
    builder = dremio_client.table("my_table")
    sql = builder.select("A").mutate(B="A+1")._compile_sql()
    expected = 'SELECT A, A+1 AS "B" FROM my_table'
    assert sql == expected

def test_mutate_no_select(dremio_client):
    # Test no select: mutate(A="B+1") -> SELECT *, B+1 AS "A"
    builder = dremio_client.table("my_table")
    sql = builder.mutate(A="B+1")._compile_sql()
    expected = 'SELECT *, B+1 AS "A" FROM my_table'
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

def test_standard_sql_features(dremio_client):
    dremio_client.pat = "mock_pat"
    builder = dremio_client.table("dummy")
    builder._execute_flight = lambda sql, lib: sql # Mock execution
    
    # Test Group By & Agg
    sql = builder.group_by("state").agg(avg_pop="AVG(pop)")._compile_sql()
    assert "GROUP BY state" in sql
    assert 'AVG(pop) AS "avg_pop"' in sql
    
    # Test Order By
    builder = dremio_client.table("dummy")
    sql = builder.order_by("pop", ascending=False)._compile_sql()
    assert "ORDER BY pop DESC" in sql
    
    # Test Distinct
    builder = dremio_client.table("dummy")
    sql = builder.select("state").distinct()._compile_sql()
    assert "SELECT DISTINCT state" in sql

def test_joins(dremio_client):
    dremio_client.pat = "mock_pat"
    left = dremio_client.table("left_table")
    left._execute_flight = lambda sql, lib: sql
    
    # Test Join with string
    joined = left.join("right_table", on="left_tbl.id = right_tbl.id", how="left")
    sql = joined._compile_sql()
    # The join method creates a new builder with initial_sql set to the join query.
    # _compile_sql on that new builder wraps it in "SELECT * FROM (...) AS sub"
    assert "LEFT JOIN" in sql
    assert "left_table" in sql
    assert "right_table" in sql
    
    # Test Join with Builder
    right = dremio_client.table("right_table").filter("active = true")
    joined2 = left.join(right, on="left_tbl.id = right_tbl.id")
    sql2 = joined2._compile_sql()
    assert "INNER JOIN" in sql2
    assert "WHERE active = true" in sql2

def test_iceberg_features(dremio_client):
    dremio_client.pat = "mock_pat"
    builder = dremio_client.table("iceberg_table")
    builder._execute_dml = lambda sql: sql
    
    # Time Travel
    sql = builder.at_snapshot("12345")._compile_sql()
    assert "FROM iceberg_table AT SNAPSHOT '12345'" in sql
    
    sql = builder.at_timestamp("2023-01-01")._compile_sql()
    assert "FROM iceberg_table AT TIMESTAMP '2023-01-01'" in sql
    
    # Maintenance
    sql = builder.optimize(min_input_files=5)
    assert "OPTIMIZE TABLE iceberg_table REWRITE DATA (MIN_INPUT_FILES=5)" in sql
    
    sql = builder.vacuum(retain_last=10)
    assert "VACUUM TABLE iceberg_table EXPIRE SNAPSHOTS RETAIN_LAST 10" in sql

# def test_external_query(dremio_client):
#     dremio_client.pat = "mock_pat"
#     
#     # Test basic external query
#     builder = dremio_client.external_query("Postgres", "SELECT * FROM users")
#     sql = builder._compile_sql()
#     # It wraps it in a subquery
#     assert "SELECT * FROM (SELECT * FROM TABLE(Postgres.EXTERNAL_QUERY('SELECT * FROM users'))) AS sub" in sql
#     
#     # Test escaping
#     builder = dremio_client.external_query("Postgres", "SELECT * FROM users WHERE name = 'Alice'")
#     sql = builder._compile_sql()
#     assert "name = ''Alice''" in sql
