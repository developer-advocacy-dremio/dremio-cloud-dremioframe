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
    with patch("dremioframe.orchestration.backend.psycopg2") as mock:
        yield mock

@pytest.fixture
def mock_mysql_connector():
    with patch("dremioframe.orchestration.backend.mysql.connector") as mock:
        yield mock

class TestPostgresBackend:
    def test_init(self, mock_psycopg2):
        # Setup mock cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        backend = PostgresBackend(dsn="postgresql://user:pass@localhost/db")
        
        # Verify table creation
        assert mock_cursor.execute.call_count >= 1
        assert "CREATE TABLE IF NOT EXISTS dremioframe_runs" in mock_cursor.execute.call_args_list[0][0][0]

    def test_save_run(self, mock_psycopg2):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        backend = PostgresBackend(dsn="postgresql://user:pass@localhost/db")
        run = get_sample_run()
        backend.save_run(run)

        # Verify insert/upsert
        assert "INSERT INTO dremioframe_runs" in mock_cursor.execute.call_args_list[1][0][0] # 0 is init, 1 is save

    def test_get_run(self, mock_psycopg2):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock return value
        mock_cursor.fetchone.return_value = ("run_123", "test_pipeline", 100.0, None, "RUNNING", {"task1": "PENDING"})

        backend = PostgresBackend(dsn="postgresql://user:pass@localhost/db")
        run = backend.get_run("run_123")

        assert run is not None
        assert run.run_id == "run_123"
        assert run.tasks["task1"] == "PENDING"

class TestMySQLBackend:
    def test_init(self, mock_mysql_connector):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_mysql_connector.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        config = {"user": "u", "password": "p", "database": "d"}
        backend = MySQLBackend(config=config)
        
        assert "CREATE TABLE IF NOT EXISTS dremioframe_runs" in mock_cursor.execute.call_args_list[0][0][0]

    def test_save_run(self, mock_mysql_connector):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_mysql_connector.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        config = {"user": "u", "password": "p", "database": "d"}
        backend = MySQLBackend(config=config)
        run = get_sample_run()
        backend.save_run(run)

        assert "INSERT INTO dremioframe_runs" in mock_cursor.execute.call_args_list[1][0][0]

    def test_update_task_status(self, mock_mysql_connector):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_mysql_connector.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        config = {"user": "u", "password": "p", "database": "d"}
        backend = MySQLBackend(config=config)
        backend.update_task_status("run_123", "task1", "SUCCESS")

        assert "UPDATE dremioframe_runs" in mock_cursor.execute.call_args_list[1][0][0]
        assert "JSON_SET" in mock_cursor.execute.call_args_list[1][0][0]
