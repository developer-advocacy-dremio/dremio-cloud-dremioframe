import pytest
from unittest.mock import MagicMock
from dremioframe.orchestration import OptimizeTask, VacuumTask, ExpireSnapshotsTask

def test_optimize_task_sql():
    mock_client = MagicMock()
    task = OptimizeTask("opt", mock_client, "my_table")
    assert task.sql == "OPTIMIZE TABLE my_table REWRITE DATA USING BIN_PACK"

def test_vacuum_task_sql():
    mock_client = MagicMock()
    task = VacuumTask("vac", mock_client, "my_table", retain_last=10)
    assert task.sql == "VACUUM TABLE my_table EXPIRE SNAPSHOTS RETAIN LAST 10"

def test_expire_snapshots_task_sql():
    mock_client = MagicMock()
    task = ExpireSnapshotsTask("exp", mock_client, "my_table", retain_last=3)
    assert task.sql == "VACUUM TABLE my_table EXPIRE SNAPSHOTS RETAIN LAST 3"
