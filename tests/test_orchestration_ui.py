import pytest
import json
import threading
import time
import requests
from dremioframe.orchestration.ui import start_ui
from dremioframe.orchestration.backend import InMemoryBackend, PipelineRun

def test_ui_api():
    # Setup backend with some data
    backend = InMemoryBackend()
    run = PipelineRun(
        pipeline_name="ui_test",
        run_id="run_ui_1",
        start_time=time.time(),
        status="SUCCESS",
        tasks={"t1": "SUCCESS"}
    )
    backend.save_run(run)
    
    # Start server in a thread
    import socket
    # Find a free port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        port = s.getsockname()[1]
        
    server_thread = threading.Thread(target=start_ui, args=(backend,), kwargs={"port": port})
    server_thread.daemon = True
    server_thread.start()
    
    # Give it a moment to start
    time.sleep(1)
    
    try:
        # Test API
        resp = requests.get(f"http://localhost:{port}/api/runs")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["pipeline_name"] == "ui_test"
        
        # Test HTML
        resp = requests.get(f"http://localhost:{port}/")
        assert resp.status_code == 200
        assert "<title>DremioFrame Orchestration</title>" in resp.text
    finally:
        # We can't easily stop the serve_forever loop from here without complex logic
        # but since it's a daemon thread it will die when test process ends.
        pass
