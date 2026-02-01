
import unittest
from unittest.mock import patch, MagicMock
from dremioframe.client import DremioClient
import os

class TestAdvancedAuth(unittest.TestCase):
    
    @patch('dremioframe.client.requests.Session')
    @patch('dremioframe.client.requests.post')
    def test_pat_exchange_cloud(self, mock_post, mock_session):
        # Setup mock for PAT exchange
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "mock_oauth_token"}
        mock_post.return_value = mock_response

        # Test Cloud Mode with PAT
        client = DremioClient(pat="test_pat", project_id="test_pid", mode="cloud")
        
        # Verify exchange called
        mock_post.assert_called_with(
            "https://api.dremio.cloud/oauth/token",
            json={
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "subject_token": "test_pat",
                "subject_token_type": "urn:ietf:params:oauth:token-type:access_token"
            },
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        # Verify header updated with OAuth token, NOT PAT
        self.assertIn("Bearer mock_oauth_token", client.session.headers["Authorization"])

    @patch('dremioframe.client.requests.post')
    def test_pat_exchange_fallback(self, mock_post):
        # Setup mock to fail
        mock_post.side_effect = Exception("Exchange Failed")

        # Test Cloud Mode with PAT
        # Should catch exception and fallback
        client = DremioClient(pat="test_pat", project_id="test_pid", mode="cloud")
        
        # Verify fallback to PAT
        self.assertIn("Bearer test_pat", client.session.headers["Authorization"])

    @patch('dremioframe.profile.get_profile_config')
    @patch('dremioframe.client.requests.post')
    def test_oauth_profile(self, mock_post, mock_get_profile):
        # Mock Profile
        mock_get_profile.return_value = {
            "type": "cloud",
            "project_id": "pid",
            "auth": {
                "type": "oauth",
                "client_id": "cid",
                "client_secret": "csecret"
            }
        }
        
        # Mock OAuth login response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "oauth_token_from_creds"}
        mock_post.return_value = mock_response

        # Init with profile
        client = DremioClient(profile="oauth_test")
        
        # Verify correct flow triggered
        self.assertEqual(client.pat, "oauth_token_from_creds")
        self.assertIn("Bearer oauth_token_from_creds", client.session.headers["Authorization"])

if __name__ == '__main__':
    unittest.main()
