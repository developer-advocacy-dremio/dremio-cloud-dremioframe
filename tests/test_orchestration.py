import pytest
import time
from unittest.mock import MagicMock, patch
from dremioframe.orchestration import Task, Pipeline, DataQualityTask

def test_task_execution():
    def add(a, b):
        return a + b
    
    task = Task("add_task", add, args=(1, 2))
    result = task.run()
    assert result == 3
    assert task.status == "SUCCESS"

def test_pipeline_dag_execution():
    results = {}
    
    def task_a():
        results['a'] = 1
        return 1
        
    def task_b(val):
        results['b'] = val + 1
        return val + 1
        
    def task_c(val):
        results['c'] = val * 2
        return val * 2
        
    t1 = Task("t1", task_a)
    t2 = Task("t2", task_b, args=(1,)) # Hardcoded arg for simplicity in this test
    t3 = Task("t3", task_c, args=(2,))
    
    # t1 -> t2 -> t3
    t1.set_downstream(t2)
    t2.set_downstream(t3)
    
    pipeline = Pipeline("test_pipeline")
    pipeline.add_task(t1).add_task(t2).add_task(t3)
    
    pipeline.run()
    
    assert results['a'] == 1
    assert results['b'] == 2
    assert results['c'] == 4
    assert t1.status == "SUCCESS"
    assert t2.status == "SUCCESS"
    assert t3.status == "SUCCESS"

def test_pipeline_cycle_detection():
    t1 = Task("t1", lambda: None)
    t2 = Task("t2", lambda: None)
    
    t1.set_downstream(t2)
    t2.set_downstream(t1)
    
    pipeline = Pipeline("cycle_pipeline")
    pipeline.add_task(t1).add_task(t2)
    
    with pytest.raises(ValueError, match="Pipeline contains a cycle"):
        pipeline.run()

@patch("dremioframe.dq.runner.DQRunner")
def test_data_quality_task_success(MockRunner):
    client = MagicMock()
    # Mock sys.modules for DQRunner import
    with patch.dict("sys.modules", {"dremioframe.dq.runner": MagicMock(DQRunner=MockRunner)}):
        runner_instance = MockRunner.return_value
        runner_instance.run_tests.return_value = True
        
        task = DataQualityTask("dq_task", client, tests=[{"name": "t1"}])
        task.run({})
        
        assert task.status == "SUCCESS"

@patch("dremioframe.dq.runner.DQRunner")
def test_data_quality_task_failure(MockRunner):
    client = MagicMock()
    with patch.dict("sys.modules", {"dremioframe.dq.runner": MagicMock(DQRunner=MockRunner)}):
        runner_instance = MockRunner.return_value
        runner_instance.run_tests.return_value = False
        
        task = DataQualityTask("dq_task", client, tests=[{"name": "t1"}])
        
        with pytest.raises(RuntimeError):
            task.run({})
    
    assert task.status == "FAILED"

def test_pipeline_stop_on_failure():
    def fail():
        raise Exception("Boom")
        
    def success():
        return "Success"
        
    t1 = Task("t1", fail)
    t2 = Task("t2", success)
    
    t1.set_downstream(t2)
    
    pipeline = Pipeline("fail_pipeline")
    pipeline.add_task(t1).add_task(t2)
    
    pipeline.run()
    
    assert t1.status == "FAILED"
    assert t2.status == "SKIPPED"
