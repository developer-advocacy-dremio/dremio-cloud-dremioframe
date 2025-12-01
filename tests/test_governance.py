import pytest
import os
from dremioframe.client import DremioClient
from dremioframe.admin import Admin

from dotenv import load_dotenv

load_dotenv()

# Use 'target' space as requested, or env var
TEST_SPACE = os.environ.get("DREMIO_TEST_SPACE", "testing")

@pytest.fixture
def client():
    pat = os.environ.get("DREMIO_PAT")
    project_id = os.environ.get("DREMIO_PROJECT_ID")
    if not pat:
        pytest.skip("DREMIO_PAT not set")
    return DremioClient(pat=pat, project_id=project_id)

@pytest.fixture
def admin(client):
    return Admin(client)

def test_udf_lifecycle(admin):
    udf_name = f"{TEST_SPACE}.test_udf_lifecycle"
    
    # Clean up first
    try:
        admin.drop_udf(udf_name, if_exists=True)
    except:
        pass

    # Create
    admin.create_udf(udf_name, "x INT", "INT", "x + 1")
    
    # Verify existence (by trying to replace it or just run it? Dremio doesn't have easy "show function" for specific one via SQL easily parsable)
    # We can try to use it in a query
    res = admin.client.sql(f"SELECT {udf_name}(1)").collect("pandas")
    assert res.iloc[0,0] == 2

    # Replace
    admin.create_udf(udf_name, "x INT", "INT", "x + 2", replace=True)
    res = admin.client.sql(f"SELECT {udf_name}(1)").collect("pandas")
    assert res.iloc[0,0] == 3

    # Drop
    admin.drop_udf(udf_name)
    
    # Verify dropped
    with pytest.raises(Exception):
        admin.client.sql(f"SELECT {udf_name}(1)").collect("pandas")

def test_masking_and_row_access(admin):
    table_name = f"{TEST_SPACE}.test_policy_table"
    masking_udf = f"{TEST_SPACE}.test_mask_udf"
    row_udf = f"{TEST_SPACE}.test_row_udf"

    # Cleanup
    try:
        admin.client.execute(f"DROP TABLE IF EXISTS {table_name}")
        admin.drop_udf(masking_udf, if_exists=True)
        admin.drop_udf(row_udf, if_exists=True)
    except:
        pass

    # 1. Create UDFs
    # Masking: if value is 'secret', show '***', else show value
    admin.create_udf(masking_udf, "val VARCHAR", "VARCHAR", "CASE WHEN val = 'secret' THEN '***' ELSE val END")
    
    # Row Access: only show rows where id > 0
    admin.create_udf(row_udf, "id INT", "BOOLEAN", "id > 0")

    # 2. Create Table
    admin.client.execute(f"CREATE TABLE {table_name} (id INT, val VARCHAR)")
    admin.client.execute(f"INSERT INTO {table_name} VALUES (1, 'public'), (2, 'secret'), (-1, 'hidden')")

    # 3. Apply Row Access Policy
    admin.apply_row_access_policy(table_name, f"{row_udf}(id)")

    # Verify Row Access
    # Should see 1 and 2. -1 should be hidden.
    res = admin.client.sql(f"SELECT * FROM {table_name} ORDER BY id").collect("pandas")
    assert len(res) == 2
    assert 1 in res['id'].values
    assert 2 in res['id'].values
    assert -1 not in res['id'].values

    # 4. Apply Masking Policy
    admin.apply_masking_policy(table_name, "val", f"{masking_udf}(val)")

    # Verify Masking
    # Row 2 val should be '***'
    res = admin.client.sql(f"SELECT * FROM {table_name} ORDER BY id").collect("pandas")
    row2 = res[res['id'] == 2].iloc[0]
    assert row2['val'] == '***'
    row1 = res[res['id'] == 1].iloc[0]
    assert row1['val'] == 'public'

    # 5. Drop Policies
    # Masking drop takes only name, no args
    admin.drop_masking_policy(table_name, "val", masking_udf)
    admin.drop_row_access_policy(table_name, f"{row_udf}(id)")

    # Verify Dropped
    res = admin.client.sql(f"SELECT * FROM {table_name} ORDER BY id").collect("pandas")
    assert len(res) == 3 # -1 is back
    row2 = res[res['id'] == 2].iloc[0]
    assert row2['val'] == 'secret' # secret is back

    # Cleanup
    admin.client.execute(f"DROP TABLE {table_name}")
    admin.drop_udf(masking_udf)
    admin.drop_udf(row_udf)
