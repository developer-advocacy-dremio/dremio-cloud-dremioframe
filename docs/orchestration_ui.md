# Orchestration Web UI

`dremioframe` includes a lightweight Web UI to visualize pipeline runs and task statuses.

## Features
-   **Dashboard**: View a list of recent pipeline runs.
-   **Real-time Updates**: The UI polls for updates every 5 seconds.
-   **Task Status**: See the status (SUCCESS, FAILED, RUNNING, SKIPPED) of each task in a run.

## Usage

To use the UI, you must use a persistent backend (e.g., `SQLiteBackend`) so the UI server can read the state.

```python
import threading
from dremioframe.orchestration import Pipeline
from dremioframe.orchestration.backend import SQLiteBackend
from dremioframe.orchestration.ui import start_ui

# 1. Initialize Backend
backend = SQLiteBackend("pipeline.db")

# 2. Start UI Server (in a separate thread or process)
# Note: In production, you might run this as a separate script.
ui_thread = threading.Thread(target=start_ui, args=(backend, 8080))
ui_thread.daemon = True
ui_thread.start()

# 3. Run Pipeline
pipeline = Pipeline("my_pipeline", backend=backend)
# ... add tasks ...
pipeline.run()
```

Access the UI at `http://localhost:8080`.
