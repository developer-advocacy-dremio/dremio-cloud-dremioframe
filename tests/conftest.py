import pytest
import os
from dotenv import load_dotenv
from dremioframe.client import DremioClient

load_dotenv()

@pytest.fixture
def dremio_client():
    pat = os.getenv("DREMIO_PAT")
    project_id = os.getenv("DREMIO_PROJECT_ID")
    if not pat:
        pytest.skip("DREMIO_PAT not set")
    return DremioClient(pat=pat, project_id=project_id)
