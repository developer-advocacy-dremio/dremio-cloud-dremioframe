import sys
from unittest.mock import MagicMock, patch

# Mock apscheduler modules BEFORE importing scheduling
mock_apscheduler = MagicMock()
sys.modules["apscheduler"] = mock_apscheduler
sys.modules["apscheduler.schedulers.background"] = mock_apscheduler
sys.modules["apscheduler.schedulers.blocking"] = mock_apscheduler
sys.modules["apscheduler.triggers.cron"] = mock_apscheduler
sys.modules["apscheduler.triggers.interval"] = mock_apscheduler
sys.modules["apscheduler.jobstores.sqlalchemy"] = mock_apscheduler

# Now import
import dremioframe.orchestration.scheduling
from dremioframe.orchestration.scheduling import schedule_pipeline
from dremioframe.orchestration.pipeline import Pipeline

class TestScheduler:
    def test_schedule_interval(self):
        pipeline = Pipeline("test_pipe")
        
        # We need to reload or ensure the import picked up the mocks.
        # Since we set sys.modules before import, it should be fine.
        # However, if it was already imported by another test, we might need to reload.
        import importlib
        import dremioframe.orchestration.scheduling
        importlib.reload(dremioframe.orchestration.scheduling)
        from dremioframe.orchestration.scheduling import schedule_pipeline
        
        schedule_pipeline(pipeline, interval_seconds=60, blocking=True)
        
        # Verify BlockingScheduler was called
        dremioframe.orchestration.scheduling.BlockingScheduler.assert_called_once()

    def test_schedule_cron(self):
        pipeline = Pipeline("test_pipe")
        
        schedule_pipeline(pipeline, cron="* * * * *", blocking=False)
        
        dremioframe.orchestration.scheduling.CronTrigger.from_crontab.assert_called_with("* * * * *")
        dremioframe.orchestration.scheduling.BackgroundScheduler.assert_called_once()

    def test_persistent_store(self):
        pipeline = Pipeline("test_pipe")
        
        schedule_pipeline(pipeline, interval_seconds=60, job_store_url="sqlite:///jobs.db")
        
        dremioframe.orchestration.scheduling.SQLAlchemyJobStore.assert_called_with(url="sqlite:///jobs.db")
