import pytest
import sys
from unittest.mock import MagicMock, patch

# Mock yaml before import (needed for DQRunner)
mock_yaml = MagicMock()
sys.modules["yaml"] = mock_yaml

from dremioframe.orchestration.tasks.dq_task import DataQualityTask

class TestDataQualityTask:
    def test_init_validation(self):
        client = MagicMock()
        with pytest.raises(ValueError):
            DataQualityTask("test", client)

    @patch("dremioframe.dq.runner.DQRunner")
    def test_run_success(self, MockRunner):
        client = MagicMock()
        task = DataQualityTask("test", client, directory="tests")
        
        # We need to patch sys.modules to ensure the import inside run() uses our mock
        with patch.dict("sys.modules", {"dremioframe.dq.runner": MagicMock(DQRunner=MockRunner)}):
            runner_instance = MockRunner.return_value
            runner_instance.load_tests.return_value = [{"name": "t1"}]
            runner_instance.run_tests.return_value = True
            
            task.run({})
            
            runner_instance.load_tests.assert_called_with("tests")
            runner_instance.run_tests.assert_called()

    @patch("dremioframe.dq.runner.DQRunner")
    def test_run_failure(self, MockRunner):
        client = MagicMock()
        task = DataQualityTask("test", client, directory="tests")
        
        with patch.dict("sys.modules", {"dremioframe.dq.runner": MagicMock(DQRunner=MockRunner)}):
            runner_instance = MockRunner.return_value
            runner_instance.load_tests.return_value = [{"name": "t1"}]
            runner_instance.run_tests.return_value = False
            
            with pytest.raises(RuntimeError):
                task.run({})
