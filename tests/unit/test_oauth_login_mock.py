
import unittest
from unittest.mock import patch, MagicMock
from dremioframe.client import DremioClient

class TestOAuthLogin(unittest.TestCase):

    @patch('dremioframe.client.requests.post')
    def test_oauth_login_success_cloud(self, mock_post):
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "mock_token_123",
            "expires_in": 3600
        }
        mock_post.return_value = mock_response

        # Init client with credentials
        client = DremioClient(
            mode="cloud",
            client_id="my_id",
            client_secret="my_secret"
        )

        # Verify PAT and Header were set
        self.assertEqual(client.pat, "mock_token_123")
        self.assertEqual(client.session.headers["Authorization"], "Bearer mock_token_123")

        # Verify correct endpoint and payload were used
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertEqual(args[0], "https://api.dremio.cloud/oauth/token")
        self.assertEqual(kwargs['data']['grant_type'], "client_credentials")
        self.assertIsNotNone(kwargs.get('auth')) # Basic Auth used

    @patch('dremioframe.client.requests.post')
    def test_oauth_login_success_software(self, mock_post):
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "software_token_456",
            "expires_in": 3600
        }
        mock_post.return_value = mock_response

        # Init client with software mode
        client = DremioClient(
            mode="v26",
            hostname="dremio.local",
            client_id="my_id",
            client_secret="my_secret"
        )

        # Verify PAT and Header
        self.assertEqual(client.pat, "software_token_456")
        
        # Verify endpoint (Software uses base_url/oauth/token)
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        # Base URL for v26 is https://dremio.local:443/api/v3
        self.assertEqual(args[0], "https://dremio.local:443/api/v3/oauth/token")

if __name__ == '__main__':
    unittest.main()
