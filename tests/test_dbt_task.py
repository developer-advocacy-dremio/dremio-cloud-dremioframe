import pytest
from dremioframe.orchestration.tasks.dbt_task import DbtTask
from unittest.mock import MagicMock, patch

def test_dbt_task_run():
    task = DbtTask("dbt_run", command="run", project_dir="/tmp/dbt")
    
    with patch('subprocess.Popen') as mock_popen:
        mock_process = MagicMock()
        mock_process.communicate.return_value = ("Done", "")
        mock_process.returncode = 0
        mock_popen.return_value = mock_process
        
        task.run({})
        
        mock_popen.assert_called_once()
        args = mock_popen.call_args[0][0]
        assert args == ["dbt", "run", "--project-dir", "/tmp/dbt"]

def test_dbt_task_failure():
    task = DbtTask("dbt_fail")
    
    with patch('subprocess.Popen') as mock_popen:
        mock_process = MagicMock()
        mock_process.communicate.return_value = ("", "Error")
        mock_process.returncode = 1
        mock_popen.return_value = mock_process
        
        with pytest.raises(Exception, match="dbt command failed"):
            task.run({})
