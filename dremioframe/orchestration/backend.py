import abc
import sqlite3
import json
import time
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict

@dataclass
class PipelineRun:
    pipeline_name: str
    run_id: str
    start_time: float
    status: str # RUNNING, SUCCESS, FAILED
    end_time: Optional[float] = None
    tasks: Dict[str, str] = None # task_name -> status

class BaseBackend(abc.ABC):
    """Abstract base class for orchestration backends."""

    @abc.abstractmethod
    def save_run(self, run: PipelineRun):
        """Saves or updates a pipeline run."""
        pass

    @abc.abstractmethod
    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        """Retrieves a pipeline run by ID."""
        pass
    
    @abc.abstractmethod
    def update_task_status(self, run_id: str, task_name: str, status: str):
        """Updates the status of a specific task in a run."""
        pass

    @abc.abstractmethod
    def list_runs(self, pipeline_name: str = None, limit: int = 10) -> List[PipelineRun]:
        """Lists recent pipeline runs."""
        pass

class InMemoryBackend(BaseBackend):
    """In-memory backend (default). State is lost when process exits."""
    
    def __init__(self):
        self.runs: Dict[str, PipelineRun] = {}

    def save_run(self, run: PipelineRun):
        self.runs[run.run_id] = run

    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        return self.runs.get(run_id)

    def update_task_status(self, run_id: str, task_name: str, status: str):
        run = self.runs.get(run_id)
        if run:
            if run.tasks is None:
                run.tasks = {}
            run.tasks[task_name] = status

    def list_runs(self, pipeline_name: str = None, limit: int = 10) -> List[PipelineRun]:
        runs = list(self.runs.values())
        if pipeline_name:
            runs = [r for r in runs if r.pipeline_name == pipeline_name]
        # Sort by start time desc
        runs.sort(key=lambda x: x.start_time, reverse=True)
        return runs[:limit]

class SQLiteBackend(BaseBackend):
    """SQLite backend for persistent state."""
    
    def __init__(self, db_path: str = "dremioframe_orchestration.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    pipeline_name TEXT,
                    start_time REAL,
                    end_time REAL,
                    status TEXT,
                    tasks TEXT
                )
            """)

    def save_run(self, run: PipelineRun):
        tasks_json = json.dumps(run.tasks) if run.tasks else "{}"
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO runs (run_id, pipeline_name, start_time, end_time, status, tasks)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (run.run_id, run.pipeline_name, run.start_time, run.end_time, run.status, tasks_json))

    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,))
            row = cursor.fetchone()
            if row:
                return PipelineRun(
                    run_id=row[0],
                    pipeline_name=row[1],
                    start_time=row[2],
                    end_time=row[3],
                    status=row[4],
                    tasks=json.loads(row[5]) if row[5] else {}
                )
        return None

    def update_task_status(self, run_id: str, task_name: str, status: str):
        # SQLite doesn't support partial JSON updates easily without extensions,
        # so we read-modify-write. Transactional safety is important here.
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT tasks FROM runs WHERE run_id = ?", (run_id,))
            row = cursor.fetchone()
            if row:
                tasks = json.loads(row[0]) if row[0] else {}
                tasks[task_name] = status
                conn.execute("UPDATE runs SET tasks = ? WHERE run_id = ?", (json.dumps(tasks), run_id))

    def list_runs(self, pipeline_name: str = None, limit: int = 10) -> List[PipelineRun]:
        query = "SELECT * FROM runs"
        params = []
        if pipeline_name:
            query += " WHERE pipeline_name = ?"
            params.append(pipeline_name)
        
        query += " ORDER BY start_time DESC LIMIT ?"
        params.append(limit)
        
        runs = []
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, params)
            for row in cursor:
                runs.append(PipelineRun(
                    run_id=row[0],
                    pipeline_name=row[1],
                    start_time=row[2],
                    end_time=row[3],
                    status=row[4],
                    tasks=json.loads(row[5]) if row[5] else {}
                ))
        return runs
