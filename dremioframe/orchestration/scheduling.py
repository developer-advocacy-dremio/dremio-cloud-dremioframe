import sched
import time
from typing import Callable, Union
from datetime import datetime, timedelta
from .pipeline import Pipeline

class CronScheduler:
    """
    Simple Cron Scheduler. Supports standard 5-field cron expressions:
    * * * * *
    | | | | |
    | | | | +----- Day of week (0 - 6) (Sunday=0)
    | | | +------- Month (1 - 12)
    | | +--------- Day of month (1 - 31)
    | +----------- Hour (0 - 23)
    +------------- Minute (0 - 59)
    """
    def __init__(self, cron_expression: str):
        self.cron_expression = cron_expression
        self.fields = cron_expression.split()
        if len(self.fields) != 5:
            raise ValueError("Invalid cron expression. Must have 5 fields.")

    def _match(self, value: int, field: str) -> bool:
        if field == "*":
            return True
        if "," in field:
            return any(self._match(value, part) for part in field.split(","))
        if "/" in field:
            base, step = field.split("/")
            step = int(step)
            if base == "*":
                return value % step == 0
            # Handle range/step e.g. 0-23/2
            if "-" in base:
                start, end = map(int, base.split("-"))
                return start <= value <= end and (value - start) % step == 0
            return int(base) == value and value % step == 0
        if "-" in field:
            start, end = map(int, field.split("-"))
            return start <= value <= end
        return int(field) == value

    def next_run(self, now: datetime = None) -> float:
        """Calculates the delay in seconds until the next run."""
        if now is None:
            now = datetime.now()
        
        # Start checking from the next minute
        candidate = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        
        while True:
            # Check if candidate matches cron
            # Re-check weekday logic
            # Python: Mon=0, Tue=1, ..., Sun=6
            # Cron: Sun=0, Mon=1, ..., Sat=6
            # So: (python + 1) % 7
            
            cron_dow = (candidate.weekday() + 1) % 7
            if (self._match(candidate.minute, self.fields[0]) and
                self._match(candidate.hour, self.fields[1]) and
                self._match(candidate.day, self.fields[2]) and
                self._match(candidate.month, self.fields[3]) and
                self._match(cron_dow, self.fields[4])):
                 return (candidate - now).total_seconds()
            
            candidate += timedelta(minutes=1)
            # Safety break for infinite loops (e.g. impossible dates)
            if (candidate - now).days > 365:
                raise ValueError("Could not find next run time within a year.")

def schedule_pipeline(pipeline: Pipeline, interval_seconds: float = None, cron: str = None):
    """
    Schedules a pipeline to run periodically.
    
    Args:
        pipeline: The pipeline to run.
        interval_seconds: Run every X seconds.
        cron: Run according to cron expression.
    """
    if not interval_seconds and not cron:
        raise ValueError("Must specify either interval_seconds or cron.")
        
    scheduler = sched.scheduler(time.time, time.sleep)
    
    def run_job():
        try:
            pipeline.run()
        except Exception as e:
                return

        sc.enter(interval_seconds, 1, run_job, (sc,))

    # Schedule first run immediately? Or after interval? Let's do immediately.
    s.enter(0, 1, run_job, (s,))
    print(f"Scheduler started for pipeline {pipeline.name}. Interval: {interval_seconds}s")
    s.run()
