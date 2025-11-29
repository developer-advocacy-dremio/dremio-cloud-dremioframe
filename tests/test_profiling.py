import pytest
from unittest.mock import MagicMock, patch
from dremioframe.profile import QueryProfile
from dremioframe.admin import Admin

@pytest.fixture
def sample_profile_data():
    return {
        "jobId": {"id": "job123"},
        "startTime": 1600000000000,
        "endTime": 1600000005000,
        "state": "COMPLETED"
    }

def test_profile_attributes(sample_profile_data):
    profile = QueryProfile(sample_profile_data)
    assert profile.job_id == "job123"
    assert profile.start_time == 1600000000000
    assert profile.end_time == 1600000005000
    assert profile.state == "COMPLETED"

def test_profile_summary(sample_profile_data, capsys):
    profile = QueryProfile(sample_profile_data)
    profile.summary()
    captured = capsys.readouterr()
    assert "Job ID: job123" in captured.out
    assert "State: COMPLETED" in captured.out

def test_profile_visualize(sample_profile_data):
    profile = QueryProfile(sample_profile_data)
    
    with patch("plotly.express.timeline") as mock_timeline:
        mock_fig = MagicMock()
        mock_timeline.return_value = mock_fig
        
        fig = profile.visualize()
        
        mock_timeline.assert_called()
        assert fig == mock_fig

def test_admin_get_profile():
    mock_client = MagicMock()
    mock_client.base_url = "http://localhost:9047/api/v3"
    mock_client.session.get.return_value.json.return_value = {"jobId": {"id": "job123"}}
    
    admin = Admin(mock_client)
    profile = admin.get_job_profile("job123")
    
    mock_client.session.get.assert_called_with("http://localhost:9047/api/v3/job/job123")
    assert isinstance(profile, QueryProfile)
    assert profile.job_id == "job123"
