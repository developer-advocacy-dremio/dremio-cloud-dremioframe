import pytest
from dremioframe.orchestration import Task, Pipeline

def test_trigger_rule_all_success():
    # A (Success) -> B (all_success) -> Runs
    t1 = Task("t1", lambda: "success")
    t2 = Task("t2", lambda: "success", trigger_rule="all_success")
    t1.set_downstream(t2)
    
    pipeline = Pipeline("test_all_success")
    pipeline.add_task(t1).add_task(t2)
    pipeline.run()
    
    assert t1.status == "SUCCESS"
    assert t2.status == "SUCCESS"

def test_trigger_rule_all_success_fail():
    # A (Fail) -> B (all_success) -> Skipped
    def fail(): raise ValueError("fail")
    t1 = Task("t1", fail)
    t2 = Task("t2", lambda: "success", trigger_rule="all_success")
    t1.set_downstream(t2)
    
    pipeline = Pipeline("test_all_success_fail")
    pipeline.add_task(t1).add_task(t2)
    pipeline.run()
    
    assert t1.status == "FAILED"
    assert t2.status == "SKIPPED"

def test_trigger_rule_one_failed():
    # A (Fail) -> B (one_failed) -> Runs
    def fail(): raise ValueError("fail")
    t1 = Task("t1", fail)
    t2 = Task("t2", lambda: "remediation", trigger_rule="one_failed")
    t1.set_downstream(t2)
    
    pipeline = Pipeline("test_one_failed")
    pipeline.add_task(t1).add_task(t2)
    pipeline.run()
    
    assert t1.status == "FAILED"
    assert t2.status == "SUCCESS"

def test_trigger_rule_one_failed_skip():
    # A (Success) -> B (one_failed) -> Skipped
    t1 = Task("t1", lambda: "success")
    t2 = Task("t2", lambda: "remediation", trigger_rule="one_failed")
    t1.set_downstream(t2)
    
    pipeline = Pipeline("test_one_failed_skip")
    pipeline.add_task(t1).add_task(t2)
    pipeline.run()
    
    assert t1.status == "SUCCESS"
    assert t2.status == "SKIPPED"

def test_trigger_rule_all_done():
    # A (Fail) -> B (all_done) -> Runs
    def fail(): raise ValueError("fail")
    t1 = Task("t1", fail)
    t2 = Task("t2", lambda: "cleanup", trigger_rule="all_done")
    t1.set_downstream(t2)
    
    pipeline = Pipeline("test_all_done")
    pipeline.add_task(t1).add_task(t2)
    pipeline.run()
    
    assert t1.status == "FAILED"
    assert t2.status == "SUCCESS"

def test_branching_path():
    # A (Fail) -> B (all_success) -> Skipped
    #          -> C (one_failed) -> Runs
    def fail(): raise ValueError("fail")
    t1 = Task("t1", fail)
    t2 = Task("t2", lambda: "success_path", trigger_rule="all_success")
    t3 = Task("t3", lambda: "fail_path", trigger_rule="one_failed")
    
    t1.set_downstream(t2)
    t1.set_downstream(t3)
    
    pipeline = Pipeline("test_branching")
    pipeline.add_task(t1).add_task(t2).add_task(t3)
    pipeline.run()
    
    assert t1.status == "FAILED"
    assert t2.status == "SKIPPED"
    assert t3.status == "SUCCESS"
