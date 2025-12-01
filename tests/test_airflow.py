import pytest
import sys
from unittest.mock import MagicMock, patch

# Mock airflow modules if not installed
try:
    import airflow
except ImportError:
    # Create mock modules
    sys.modules["airflow"] = MagicMock()
    sys.modules["airflow.hooks"] = MagicMock()
    sys.modules["airflow.hooks.base"] = MagicMock()
    sys.modules["airflow.models"] = MagicMock()
    sys.modules["airflow.utils"] = MagicMock()
    sys.modules["airflow.utils.decorators"] = MagicMock()
    
    # Define BaseHook and BaseOperator mocks
    class BaseHook:
        def __init__(self, **kwargs): pass
        def get_connection(self, conn_id): pass
        
    class BaseOperator:
        def __init__(self, **kwargs): 
            self.log = MagicMock()
            
    # Mock apply_defaults decorator
    def apply_defaults(func):
        return func
        
    sys.modules["airflow.hooks.base"].BaseHook = BaseHook
    sys.modules["airflow.models"].BaseOperator = BaseOperator
    sys.modules["airflow.utils.decorators"].apply_defaults = apply_defaults

from dremioframe.airflow.hooks import DremioHook
from dremioframe.airflow.operators import DremioSQLOperator, DremioDataQualityOperator

@pytest.fixture
def mock_dremio_client():
    with patch("dremioframe.airflow.hooks.DremioClient") as mock:
        client_instance = MagicMock()
        mock.return_value = client_instance
        yield client_instance

def test_dremio_hook_get_conn(mock_dremio_client):
    # Mock get_connection
    with patch.object(DremioHook, "get_connection") as mock_get_conn:
        mock_conn = MagicMock()
        mock_conn.host = "data.dremio.cloud"
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.port = 443
        mock_conn.extra_dejson = {"project_id": "123"}
        mock_get_conn.return_value = mock_conn
        
        hook = DremioHook()
        client = hook.get_conn()
        
        assert client == mock_dremio_client
        # Verify DremioClient init args
        # We can't easily verify args passed to DremioClient constructor because we mocked the class return value
        # But we can check if the class was called
        from dremioframe.airflow.hooks import DremioClient
        DremioClient.assert_called_with(
            hostname="data.dremio.cloud",
            port=443,
            username="user",
            password="pass",
            pat=None,  # pat is None when username is provided
            project_id="123",
            tls=True,
            disable_certificate_verification=False
        )

def test_dremio_sql_operator(mock_dremio_client):
    # Mock hook
    with patch("dremioframe.airflow.operators.DremioHook") as MockHook:
        hook_instance = MockHook.return_value
        # Mock get_pandas_df
        import pandas as pd
        df = pd.DataFrame({"col1": [1, 2]})
        hook_instance.get_pandas_df.return_value = df
        
        op = DremioSQLOperator(task_id="test", sql="SELECT 1")
        result = op.execute({})
        
        hook_instance.get_pandas_df.assert_called_with("SELECT 1")
        assert result is None # return_result=False by default

def test_dremio_dq_operator(mock_dremio_client):
    # Mock hook and client
    with patch("dremioframe.airflow.operators.DremioHook") as MockHook:
        hook_instance = MockHook.return_value
        hook_instance.get_conn.return_value = mock_dremio_client
        
        # Mock builder and quality
        mock_builder = MagicMock()
        mock_dremio_client.table.return_value = mock_builder
        mock_dq = MagicMock()
        mock_builder.quality = mock_dq
        
        checks = [
            {"type": "not_null", "column": "id"},
            {"type": "row_count", "expr": "val > 0", "value": 5, "op": "ge"}
        ]
        
        op = DremioDataQualityOperator(task_id="dq_check", table_name="test_table", checks=checks)
        op.execute({})
        
        mock_dq.expect_not_null.assert_called_with("id")
        mock_dq.expect_row_count.assert_called_with("val > 0", 5, "ge")
