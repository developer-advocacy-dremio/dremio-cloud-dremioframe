import pytest
from typer.testing import CliRunner
from unittest.mock import MagicMock, patch
from dremioframe.cli import app
from dremioframe.orchestration.backend import PipelineRun

runner = CliRunner()

class TestCli:
    @patch("dremioframe.orchestration.backend.SQLiteBackend")
    def test_pipeline_list(self, mock_backend_cls):
        mock_backend = MagicMock()
        mock_backend_cls.return_value = mock_backend
        
        # Mock runs
        run = PipelineRun("test_pipe", "run_1", 100.0, "SUCCESS")
        mock_backend.list_runs.return_value = [run]
        
        result = runner.invoke(app, ["pipeline", "list", "--backend-url", "sqlite:///test.db"])
        
        assert result.exit_code == 0
        assert "test_pipe" in result.stdout
        assert "run_1" in result.stdout
        
        mock_backend_cls.assert_called_with("test.db")

    @patch("dremioframe.orchestration.ui.start_ui")
    @patch("dremioframe.orchestration.backend.SQLiteBackend")
    def test_pipeline_ui(self, mock_backend_cls, mock_start_ui):
        mock_backend = MagicMock()
        mock_backend_cls.return_value = mock_backend
        
        result = runner.invoke(app, ["pipeline", "ui", "--port", "9090", "--backend-url", "sqlite:///test.db"])
        
        assert result.exit_code == 0
        assert "Starting UI on port 9090" in result.stdout
        
        mock_start_ui.assert_called_with(mock_backend, port=9090)
