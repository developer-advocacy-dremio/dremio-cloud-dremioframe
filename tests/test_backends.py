import pytest
from unittest.mock import MagicMock, patch
import json
from dremioframe.orchestration.backend import PostgresBackend, MySQLBackend, PipelineRun

# Mock PipelineRun
def get_sample_run():
    return PipelineRun(
        pipeline_name="test_pipeline",
        run_id="run_123",
        start_time=100.0,
        status="RUNNING",
        tasks={"task1": "PENDING"}
    )

@pytest.fixture
def mock_psycopg2():
    with patch.dict("sys.modules", {"psycopg2": MagicMock(), "psycopg2.extras": MagicMock()}):
        import psycopg2
        yield psycopg2

@pytest.fixture
def mock_mysql_connector():
    with patch.dict("sys.modules", {"mysql": MagicMock(), "mysql.connector": MagicMock()}):
        import mysql.connector
        yield mysql.connector

class TestPostgresBackend:
    # ... (init test unchanged)

    def test_save_run(self, mock_psycopg2):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        backend = PostgresBackend(dsn="postgresql://user:pass@localhost/db")
        run = get_sample_run()
        backend.save_run(run)

        # Verify insert/upsert
        # Check if ANY call contains INSERT
        calls = [str(call) for call in mock_cursor.execute.call_args_list]
        assert any("INSERT INTO dremioframe_runs" in c for c in calls)

# ...

class TestMySQLBackend:
    # ...

    def test_save_run(self, mock_mysql_connector):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_mysql_connector.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        config = {"user": "u", "password": "p", "database": "d"}
        backend = MySQLBackend(config=config)
        run = get_sample_run()
        backend.save_run(run)

        calls = [str(call) for call in mock_cursor.execute.call_args_list]
        assert any("INSERT INTO dremioframe_runs" in c for c in calls)

    def test_update_task_status(self, mock_mysql_connector):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_mysql_connector.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        config = {"user": "u", "password": "p", "database": "d"}
        backend = MySQLBackend(config=config)
        backend.update_task_status("run_123", "task1", "SUCCESS")

        calls = [str(call) for call in mock_cursor.execute.call_args_list]
        assert any("UPDATE dremioframe_runs" in c for c in calls)
        assert any("JSON_SET" in c for c in calls)
