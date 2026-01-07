
import os
import pytest
from dotenv import load_dotenv
from dremioframe.client import DremioClient

load_dotenv()

def test_dataframe_queries():
    print("--- Starting DataFrame API Regression Test ---")
    
    # Mode selection logic (reused from test_service_users.py)
    forced_mode = os.getenv("TEST_MODE")
    if forced_mode == "v26":
        os.environ["DREMIO_SOFTWARE_PAT"] = os.getenv("DREMIO_SOFTWARE_TOKEN")
        os.environ["DREMIO_SOFTWARE_HOST"] = os.getenv("DREMIO_SOFTWARE_BASE_URL", "").replace("/api/v3", "")
        mode = "v26"
        print("Mode: v26 (Software)")
    elif os.getenv("DREMIO_CLOUD_TOKEN"):
        os.environ["DREMIO_PAT"] = os.getenv("DREMIO_CLOUD_TOKEN")
        os.environ["DREMIO_PROJECT_ID"] = os.getenv("DREMIO_CLOUD_PROJECTID")
        mode = "cloud"
        print("Mode: cloud")
    else:
        print("Skipping: No credentials.")
        return

    try:
        client = DremioClient(mode=mode)
        # Use a simple query that works everywhere
        sql = "SELECT 1 as id, 'test' as name"
        
        print(f"1. Executing Query: {sql}")
        
        # Test 1: to_arrow (format="arrow")
        print("   Testing query(format='arrow')...")
        arrow_table = client.query(sql, format="arrow")
        print(f"   Success! Rows: {arrow_table.num_rows}")
        print(f"   Schema: {arrow_table.schema}")
        
        # Test 2: to_pandas (format="pandas")
        try:
            import pandas
            print("   Testing query(format='pandas')...")
            df = client.query(sql, format="pandas")
            print("   Success!")
            print(df.head())
        except ImportError:
            print("   Pandas not installed, skipping pandas test.")
            
        # Test 3: to_polars (format="polars")
        try:
            import polars
            print("   Testing query(format='polars')...")
            df_pl = client.query(sql, format="polars")
            print("   Success!")
            print(df_pl)
        except ImportError:
            print("   Polars not installed, skipping polars test.")

    except Exception as e:
        print(f"FAIL: {e}")
        raise e

if __name__ == "__main__":
    test_dataframe_queries()
