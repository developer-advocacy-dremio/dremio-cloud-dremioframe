import pytest
import base64
from unittest.mock import MagicMock, patch
from dremioframe.orchestration.ui import OrchestrationHandler

class TestUiSecurity:
    @patch("http.server.SimpleHTTPRequestHandler.__init__")
    def test_no_auth_required(self, mock_init):
        mock_init.return_value = None
        # Setup handler with no creds
        handler = OrchestrationHandler(MagicMock(), {}, username=None, password=None, request=MagicMock(), client_address=("", 0), server=MagicMock())
        handler.headers = {}
        
        # Should pass
        assert handler._check_auth() is True

    @patch("http.server.SimpleHTTPRequestHandler.__init__")
    def test_auth_required_missing_header(self, mock_init):
        mock_init.return_value = None
        handler = OrchestrationHandler(MagicMock(), {}, username="user", password="pass", request=MagicMock(), client_address=("", 0), server=MagicMock())
        handler.headers = {}
        
        # Should fail
        assert handler._check_auth() is False

    @patch("http.server.SimpleHTTPRequestHandler.__init__")
    def test_auth_required_invalid_creds(self, mock_init):
        mock_init.return_value = None
        handler = OrchestrationHandler(MagicMock(), {}, username="user", password="pass", request=MagicMock(), client_address=("", 0), server=MagicMock())
        
        # Encode wrong creds
        creds = base64.b64encode(b"user:wrong").decode("utf-8")
        handler.headers = {"Authorization": f"Basic {creds}"}
        
        # Should fail
        assert handler._check_auth() is False

    @patch("http.server.SimpleHTTPRequestHandler.__init__")
    def test_auth_required_valid_creds(self, mock_init):
        mock_init.return_value = None
        handler = OrchestrationHandler(MagicMock(), {}, username="user", password="pass", request=MagicMock(), client_address=("", 0), server=MagicMock())
        
        # Encode correct creds
        creds = base64.b64encode(b"user:pass").decode("utf-8")
        handler.headers = {"Authorization": f"Basic {creds}"}
        
        # Should pass
        assert handler._check_auth() is True
