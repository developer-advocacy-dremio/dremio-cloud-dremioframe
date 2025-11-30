import pytest
from unittest.mock import MagicMock, patch
import json
from dremioframe.orchestration.ui import OrchestrationHandler
from dremioframe.orchestration.backend import InMemoryBackend, PipelineRun
from dremioframe.orchestration.pipeline import Pipeline
import http.server

class MockRequest:
    def __init__(self, path, method="GET"):
        self.path = path
        self.command = method
        self.request_version = "HTTP/1.1"
    
    def makefile(self, *args, **kwargs):
        return MagicMock()

class MockServer:
    def __init__(self):
        self.server_name = "localhost"
        self.server_port = 8080

class TestOrchestrationHandler:
    def test_get_runs(self):
        backend = InMemoryBackend()
        backend.save_run(PipelineRun("p1", "r1", 100.0, "SUCCESS"))
        
        # Mock request/response
        request = MagicMock()
        client_address = ("127.0.0.1", 12345)
        server = MockServer()
        
        # We can't easily instantiate SimpleHTTPRequestHandler without a real socket
        # So we mock the handler's superclass methods or use a helper
        pass

    # Testing SimpleHTTPRequestHandler is tricky without a real socket.
    # Let's test the logic by mocking the wfile and rfile.
    
    @patch("http.server.SimpleHTTPRequestHandler.__init__")
    def test_api_runs(self, mock_init):
        mock_init.return_value = None # Skip super init
        
        backend = InMemoryBackend()
        backend.save_run(PipelineRun("p1", "r1", 100.0, "SUCCESS"))
        
        handler = OrchestrationHandler(backend, {})
        handler.path = "/api/runs"
        handler.wfile = MagicMock()
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()
        
        handler.do_GET()
        
        # Verify response
        handler.send_response.assert_called_with(200)
        handler.wfile.write.assert_called()
        
        # Check JSON content
        call_args = handler.wfile.write.call_args[0][0]
        data = json.loads(call_args.decode("utf-8"))
        assert len(data) == 1
        assert data[0]["run_id"] == "r1"

    @patch("http.server.SimpleHTTPRequestHandler.__init__")
    def test_trigger_pipeline(self, mock_init):
        mock_init.return_value = None
        
        backend = InMemoryBackend()
        pipeline = MagicMock()
        pipelines = {"p1": pipeline}
        
        handler = OrchestrationHandler(backend, pipelines)
        handler.path = "/api/pipelines/p1/trigger"
        handler.wfile = MagicMock()
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()
        
        handler.do_POST()
        
        handler.send_response.assert_called_with(200)
        pipeline.run.assert_called_once() # Actually, it's in a thread, so we might need to wait or mock threading
        
    @patch("http.server.SimpleHTTPRequestHandler.__init__")
    @patch("threading.Thread")
    def test_trigger_pipeline_threaded(self, mock_thread, mock_init):
        mock_init.return_value = None
        
        backend = InMemoryBackend()
        pipeline = MagicMock()
        pipelines = {"p1": pipeline}
        
        handler = OrchestrationHandler(backend, pipelines)
        handler.path = "/api/pipelines/p1/trigger"
        handler.wfile = MagicMock()
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()
        
        handler.do_POST()
        
        mock_thread.assert_called_once()
        # Verify target is pipeline.run
        assert mock_thread.call_args[1]['target'] == pipeline.run
