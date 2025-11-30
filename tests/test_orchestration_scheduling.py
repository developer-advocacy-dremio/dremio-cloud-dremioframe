import pytest
from datetime import datetime, timedelta
from dremioframe.orchestration.scheduling import CronScheduler

def test_cron_every_minute():
    # * * * * *
    scheduler = CronScheduler("* * * * *")
    now = datetime(2023, 1, 1, 12, 0, 0)
    delay = scheduler.next_run(now)
    # Should be 60 seconds (next minute)
    assert delay == 60.0

def test_cron_specific_minute():
    # 30 * * * * (At minute 30)
    scheduler = CronScheduler("30 * * * *")
    now = datetime(2023, 1, 1, 12, 0, 0)
    delay = scheduler.next_run(now)
    # Should be 30 minutes = 1800 seconds
    assert delay == 1800.0

def test_cron_step():
    # */5 * * * * (Every 5 minutes)
    scheduler = CronScheduler("*/5 * * * *")
    now = datetime(2023, 1, 1, 12, 2, 0) # 12:02
    delay = scheduler.next_run(now)
    # Next run at 12:05. Delay = 3 mins = 180s
    assert delay == 180.0

def test_cron_list():
    # 0,30 * * * * (At minute 0 and 30)
    scheduler = CronScheduler("0,30 * * * *")
    now = datetime(2023, 1, 1, 12, 15, 0)
    delay = scheduler.next_run(now)
    # Next run at 12:30. Delay = 15 mins = 900s
    assert delay == 900.0

def test_cron_range():
    # 0-5 * * * * (Minutes 0 through 5)
    scheduler = CronScheduler("0-5 * * * *")
    now = datetime(2023, 1, 1, 12, 3, 0)
    delay = scheduler.next_run(now)
    # Next run at 12:04. Delay = 60s
    assert delay == 60.0
