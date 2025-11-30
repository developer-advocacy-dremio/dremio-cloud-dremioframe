import http.server
import socketserver
import json
import threading
import os
from typing import Optional
from .backend import BaseBackend

class OrchestrationHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, backend: BaseBackend, *args, **kwargs):
        self.backend = backend
        # We don't want to serve files from current directory by default unless specified
        # But SimpleHTTPRequestHandler does that.
        # We will override do_GET to serve our specific endpoints.
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(self._get_html().encode("utf-8"))
        elif self.path == "/api/runs":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            runs = self.backend.list_runs(limit=50)
            # Convert runs to dicts
            runs_data = [
                {
                    "run_id": r.run_id,
                    "pipeline_name": r.pipeline_name,
                    "start_time": r.start_time,
                    "end_time": r.end_time,
                    "status": r.status,
                    "tasks": r.tasks
                }
                for r in runs
            ]
            self.wfile.write(json.dumps(runs_data).encode("utf-8"))
        else:
            self.send_error(404, "File not found")

    def _get_html(self):
        return """
<!DOCTYPE html>
<html>
<head>
    <title>DremioFrame Orchestration</title>
    <style>
        body { font-family: sans-serif; margin: 20px; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .SUCCESS { color: green; }
        .FAILED { color: red; }
        .RUNNING { color: blue; }
        .SKIPPED { color: gray; }
    </style>
</head>
<body>
    <h1>Pipeline Runs</h1>
    <button onclick="loadRuns()">Refresh</button>
    <table id="runsTable">
        <thead>
            <tr>
                <th>Pipeline</th>
                <th>Run ID</th>
                <th>Start Time</th>
                <th>Status</th>
                <th>Tasks</th>
            </tr>
        </thead>
        <tbody></tbody>
    </table>
    <script>
        async function loadRuns() {
            const response = await fetch('/api/runs');
            const runs = await response.json();
            const tbody = document.querySelector('#runsTable tbody');
            tbody.innerHTML = '';
            runs.forEach(run => {
                const tr = document.createElement('tr');
                const tasksHtml = Object.entries(run.tasks || {}).map(([name, status]) => 
                    `<span class="${status}">${name}: ${status}</span>`
                ).join(', ');
                
                tr.innerHTML = `
                    <td>${run.pipeline_name}</td>
                    <td>${run.run_id}</td>
                    <td>${new Date(run.start_time * 1000).toLocaleString()}</td>
                    <td class="${run.status}">${run.status}</td>
                    <td>${tasksHtml}</td>
                `;
                tbody.appendChild(tr);
            });
        }
        loadRuns();
        setInterval(loadRuns, 5000);
    </script>
</body>
</html>
        """

def start_ui(backend: BaseBackend, port: int = 8080):
    """
    Starts the Orchestration UI server.
    """
    # Factory to pass backend to handler
    def handler_factory(*args, **kwargs):
        return OrchestrationHandler(backend, *args, **kwargs)

    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", port), handler_factory) as httpd:
        print(f"Serving UI at http://localhost:{port}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nStopping UI server.")
            httpd.server_close()
