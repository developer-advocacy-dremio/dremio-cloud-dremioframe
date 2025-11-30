import pytest
from unittest.mock import MagicMock, patch
from dremioframe.orchestration.executors import LocalExecutor, CeleryExecutor
from dremioframe.orchestration.task import Task
from dremioframe.orchestration.backend import InMemoryBackend

def sample_task_action(context=None):
    return "result"

@pytest.fixture
def mock_backend():
    return InMemoryBackend()

class TestLocalExecutor:
    def test_submit_and_wait(self, mock_backend):
        executor = LocalExecutor(mock_backend)
        task = Task("test_task", sample_task_action)
        
        future = executor.submit_task(task, {}, "run_1")
        futures = {future: task}
        
        results = executor.wait_for_completion(futures)
        
        assert "test_task" in results
        assert results["test_task"]["status"] == "SUCCESS"
        assert results["test_task"]["result"] == "result"
        assert len(futures) == 0 # Should be popped

class TestCeleryExecutor:
    @patch("dremioframe.orchestration.executors.Celery")
    def test_init(self, mock_celery, mock_backend):
        executor = CeleryExecutor(mock_backend)
        mock_celery.assert_called_once()

    @patch("dremioframe.orchestration.executors.Celery")
    def test_submit_task(self, mock_celery, mock_backend):
        executor = CeleryExecutor(mock_backend)
        mock_app = mock_celery.return_value
        
        task = Task("test_task", sample_task_action)
        executor.submit_task(task, {}, "run_1")
        
        mock_app.send_task.assert_called_once()
        args = mock_app.send_task.call_args[1]['args']
        assert len(args) == 2 # pickled_task, context

    @patch("dremioframe.orchestration.executors.Celery")
    def test_wait_for_completion(self, mock_celery, mock_backend):
        executor = CeleryExecutor(mock_backend)
        
        mock_future = MagicMock()
        mock_future.ready.return_value = True
        mock_future.get.return_value = "result"
        
        task = Task("test_task", sample_task_action)
        futures = {mock_future: task}
        
        results = executor.wait_for_completion(futures)
        
        assert "test_task" in results
        assert results["test_task"]["status"] == "SUCCESS"
        assert results["test_task"]["result"] == "result"
