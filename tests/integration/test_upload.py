import pytest
import os
import pandas as pd
from dremioframe.client import DremioClient

# Integration tests require a live Dremio instance
# We skip if credentials are not available
@pytest.mark.skipif(not os.getenv("DREMIO_PAT"), reason="DREMIO_PAT not set")
class TestUploadIntegration:
    @pytest.fixture(scope="class")
    def client(self):
        return DremioClient()

    @pytest.fixture(scope="class")
    def test_space(self):
        # Use a temporary space or folder for testing
        # Assuming "Samples" exists or user has a scratch space
        # Let's try to use a "Scratch" space if available, or fail gracefully
        # For now, let's assume the user has configured a test location via env var
        # or we default to "test_upload" in the home space if possible?
        # Dremio Cloud usually has a home space "@user".
        return os.getenv("DREMIO_TEST_SPACE", "Samples") # Fallback to Samples might fail if read-only

    def setup_method(self):
        # Create dummy files
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        df.to_csv("test_upload.csv", index=False)
        df.to_json("test_upload.json", orient="records")
        df.to_parquet("test_upload.parquet", index=False)

    def teardown_method(self):
        # Remove dummy files
        for f in ["test_upload.csv", "test_upload.json", "test_upload.parquet"]:
            if os.path.exists(f):
                os.remove(f)

    def test_upload_csv_integration(self, client):
        # We need a writable location. 
        # Let's try to create a table in the user's home space if possible.
        # But we don't know the username easily without an API call.
        # Let's assume the user provided a writable location in DREMIO_TEST_SPACE.
        # If not, this test might fail.
        
        space = os.getenv("DREMIO_TEST_SPACE")
        if not space:
            pytest.skip("DREMIO_TEST_SPACE not set. Skipping integration test.")

        table_name = f"{space}.test_upload_csv"
        
        # Cleanup before
        try:
            client.execute(f"DROP TABLE IF EXISTS {table_name}")
        except:
            pass

        client.upload_file("test_upload.csv", table_name)
        
        # Verify
        df = client.table(table_name).collect("pandas")
        assert len(df) == 3
        assert "Alice" in df["name"].values
        
        # Cleanup after
        client.execute(f"DROP TABLE {table_name}")

    def test_upload_parquet_integration(self, client):
        space = os.getenv("DREMIO_TEST_SPACE")
        if not space:
            pytest.skip("DREMIO_TEST_SPACE not set. Skipping integration test.")

        table_name = f"{space}.test_upload_parquet"
        
        try:
            client.execute(f"DROP TABLE IF EXISTS {table_name}")
        except:
            pass

        client.upload_file("test_upload.parquet", table_name)
        
        df = client.table(table_name).collect("pandas")
        assert len(df) == 3
        
        client.execute(f"DROP TABLE {table_name}")

    def test_upload_excel_integration(self, client):
        try:
            import pandas as pd
            import openpyxl
        except ImportError:
            pytest.skip("pandas or openpyxl not installed")
            
        space = os.getenv("DREMIO_TEST_SPACE")
        if not space:
            pytest.skip("DREMIO_TEST_SPACE not set")

        # Create dummy excel
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        df.to_excel("test_upload.xlsx", index=False)
        
        table_name = f"{space}.test_upload_excel"
        try:
            client.execute(f"DROP TABLE IF EXISTS {table_name}")
        except:
            pass

        client.upload_file("test_upload.xlsx", table_name)
        
        df_res = client.table(table_name).collect("pandas")
        assert len(df_res) == 3
        
        client.execute(f"DROP TABLE {table_name}")
        os.remove("test_upload.xlsx")

    def test_upload_html_integration(self, client):
        try:
            import pandas as pd
            import lxml
        except ImportError:
            pytest.skip("pandas or lxml not installed")
            
        space = os.getenv("DREMIO_TEST_SPACE")
        if not space:
            pytest.skip("DREMIO_TEST_SPACE not set")

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        df.to_html("test_upload.html", index=False)
        
        table_name = f"{space}.test_upload_html"
        try:
            client.execute(f"DROP TABLE IF EXISTS {table_name}")
        except:
            pass

        client.upload_file("test_upload.html", table_name)
        
        df_res = client.table(table_name).collect("pandas")
        assert len(df_res) == 3
        
        client.execute(f"DROP TABLE {table_name}")
        os.remove("test_upload.html")

    def test_upload_avro_integration(self, client):
        try:
            import fastavro
        except ImportError:
            pytest.skip("fastavro not installed")
            
        space = os.getenv("DREMIO_TEST_SPACE")
        if not space:
            pytest.skip("DREMIO_TEST_SPACE not set")

        records = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}]
        schema = {
            "doc": "A weather reading.",
            "name": "Weather",
            "namespace": "test",
            "type": "record",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
            ],
        }
        with open("test_upload.avro", "wb") as out:
            fastavro.writer(out, schema, records)
        
        table_name = f"{space}.test_upload_avro"
        try:
            client.execute(f"DROP TABLE IF EXISTS {table_name}")
        except:
            pass

        client.upload_file("test_upload.avro", table_name)
        
        df_res = client.table(table_name).collect("pandas")
        assert len(df_res) == 3
        
        client.execute(f"DROP TABLE {table_name}")
        os.remove("test_upload.avro")

    def test_upload_feather_integration(self, client):
        try:
            import pyarrow.feather as feather
            import pandas as pd
        except ImportError:
            pytest.skip("pyarrow not installed")
            
        space = os.getenv("DREMIO_TEST_SPACE")
        if not space:
            pytest.skip("DREMIO_TEST_SPACE not set")

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        feather.write_feather(df, "test_upload.feather")
        
        table_name = f"{space}.test_upload_feather"
        try:
            client.execute(f"DROP TABLE IF EXISTS {table_name}")
        except:
            pass

        client.upload_file("test_upload.feather", table_name)
        
        df_res = client.table(table_name).collect("pandas")
        assert len(df_res) == 3
        
        client.execute(f"DROP TABLE {table_name}")
        os.remove("test_upload.feather")
