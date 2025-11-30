import pytest
import time
import os
from dremioframe.orchestration import Task, Pipeline, task

def test_parallel_execution():
    # Verify that tasks run in parallel by checking total time
    # If sequential: 1s + 1s = 2s
    # If parallel: max(1s, 1s) = ~1s
    
    def slow_task():
        time.sleep(1)
        return "done"
        
    t1 = Task("slow1", slow_task)
    t2 = Task("slow2", slow_task)
    
    pipeline = Pipeline("parallel_test", max_workers=2)
    pipeline.add_task(t1).add_task(t2)
    
    start = time.time()
    pipeline.run()
    end = time.time()
    
    duration = end - start
    # Allow some overhead, but it should be significantly less than 2s
    assert duration < 1.5
    assert t1.status == "SUCCESS"
    assert t2.status == "SUCCESS"

def test_context_passing():
    def producer():
        return 42
        
    def consumer(context=None):
        val = context.get("producer")
        return val + 1
        
    t1 = Task("producer", producer)
    t2 = Task("consumer", consumer)
    
    t1.set_downstream(t2)
    
    pipeline = Pipeline("context_test")
    pipeline.add_task(t1).add_task(t2)
    
    results = pipeline.run()
    
    assert results["producer"] == 42
    assert results["consumer"] == 43

def test_retries():
    attempts = 0
    def flaky_task():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError("Fail")
        return "Success"
        
    # Retry 3 times, delay 0.1s
    t1 = Task("flaky", flaky_task, retries=3, retry_delay=0.1)
    
    pipeline = Pipeline("retry_test")
    pipeline.add_task(t1)
    
    pipeline.run()
    
    assert t1.status == "SUCCESS"
    assert attempts == 3

def test_decorator():
    @task(name="decorated", retries=1)
    def my_func(x):
        return x * 2
        
    t = my_func(10)
    assert isinstance(t, Task)
    assert t.name == "decorated"
    assert t.retries == 1
    
    pipeline = Pipeline("decorator_test")
    pipeline.add_task(t)
    results = pipeline.run()
    
    assert results["decorated"] == 20

def test_visualization(tmp_path):
    t1 = Task("t1", lambda: None)
    t2 = Task("t2", lambda: None)
    t1.set_downstream(t2)
    
    pipeline = Pipeline("viz_test")
    pipeline.add_task(t1).add_task(t2)
    
    output_file = tmp_path / "graph.mermaid"
    chart = pipeline.visualize(str(output_file))
    
    assert "t1 --> t2" in chart
    assert os.path.exists(output_file)
