import sys
from unittest.mock import MagicMock, patch, mock_open

# Mock yaml before import
mock_yaml = MagicMock()
sys.modules["yaml"] = mock_yaml

from dremioframe.dq.runner import DQRunner

class TestDQRunner:
    def test_load_tests(self):
        client = MagicMock()
        runner = DQRunner(client)
        
        # Mock yaml.safe_load to return data
        mock_yaml.safe_load.return_value = [{
            "name": "Test 1",
            "table": "table1",
            "checks": [{"type": "not_null", "column": "id"}]
        }]
        
        with patch("os.path.exists", return_value=True):
            # glob is called twice (*.yaml, *.yml). Return list for first call, empty for second.
            with patch("glob.glob", side_effect=[["test.yaml"], []]):
                with patch("builtins.open", mock_open(read_data="data")):
                    tests = runner.load_tests("tests/dq")
                    
        assert len(tests) == 1
        assert tests[0]["name"] == "Test 1"
        assert tests[0]["table"] == "table1"

    def test_run_tests_success(self):
        client = MagicMock()
        runner = DQRunner(client)
        
        # Mock DataQuality
        with patch("dremioframe.dq.runner.DataQuality") as MockDQ:
            dq_instance = MockDQ.return_value
            dq_instance.expect_not_null.return_value = True
            
            tests = [{
                "name": "Test 1",
                "table": "table1",
                "checks": [{"type": "not_null", "column": "id"}]
            }]
            
            success = runner.run_tests(tests)
            
            assert success is True
            dq_instance.expect_not_null.assert_called_with("id")

    def test_run_tests_failure(self):
        client = MagicMock()
        runner = DQRunner(client)
        
        with patch("dremioframe.dq.runner.DataQuality") as MockDQ:
            dq_instance = MockDQ.return_value
            dq_instance.expect_not_null.side_effect = ValueError("Failed")
            
            tests = [{
                "name": "Test 1",
                "table": "table1",
                "checks": [{"type": "not_null", "column": "id"}]
            }]
            
            success = runner.run_tests(tests)
            
            assert success is False
