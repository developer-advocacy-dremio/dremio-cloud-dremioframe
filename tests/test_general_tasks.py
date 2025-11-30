import pytest
from unittest.mock import MagicMock, patch
from dremioframe.orchestration.tasks.general import HttpTask, EmailTask, ShellTask, S3Task

class TestHttpTask:
    @patch("requests.request")
    def test_run(self, mock_request):
        mock_response = MagicMock()
        mock_response.json.return_value = {"key": "val"}
        mock_response.content = b'{"key": "val"}'
        mock_request.return_value = mock_response
        
        task = HttpTask("http_test", "http://example.com", method="POST", json_data={"a": 1})
        result = task.run({})
        
        mock_request.assert_called_with("POST", "http://example.com", headers=None, json={"a": 1})
        assert result == {"key": "val"}

class TestEmailTask:
    @patch("smtplib.SMTP")
    def test_run(self, mock_smtp):
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server
        
        task = EmailTask("email_test", "Subject", "Body", "to@example.com")
        task.run({})
        
        mock_server.send_message.assert_called_once()

class TestShellTask:
    @patch("subprocess.run")
    def test_run(self, mock_run):
        mock_result = MagicMock()
        mock_result.stdout = "output\n"
        mock_run.return_value = mock_result
        
        task = ShellTask("shell_test", "echo hello")
        result = task.run({})
        
        mock_run.assert_called_once()
        assert result == "output"

class TestS3Task:
    def test_upload(self):
        mock_boto = MagicMock()
        mock_s3 = MagicMock()
        mock_boto.client.return_value = mock_s3
        
        with patch.dict("sys.modules", {"boto3": mock_boto}):
            task = S3Task("s3_up", "upload_file", "bucket", "key", "local.txt")
            task.run({})
            
            mock_s3.upload_file.assert_called_with("local.txt", "bucket", "key")

    def test_download(self):
        mock_boto = MagicMock()
        mock_s3 = MagicMock()
        mock_boto.client.return_value = mock_s3
        
        with patch.dict("sys.modules", {"boto3": mock_boto}):
            task = S3Task("s3_down", "download_file", "bucket", "key", "local.txt")
            task.run({})
            
            mock_s3.download_file.assert_called_with("bucket", "key", "local.txt")
