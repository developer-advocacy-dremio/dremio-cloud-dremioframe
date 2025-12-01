import pytest
import os
import uuid
from dremioframe.client import DremioClient
from dremioframe.ai.agent import DremioAgent

@pytest.fixture(scope="module")
def client():
    pat = os.getenv("DREMIO_PAT")
    if not pat:
        pytest.skip("DREMIO_PAT not set")
    return DremioClient(pat=pat)

@pytest.fixture(scope="module")
def agent():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        pytest.skip("OPENAI_API_KEY not set")
    return DremioAgent(model="gpt-4o")

def test_analyze_job_failure(client, agent):
    """
    Test that the agent can analyze a failed job.
    """
    # 1. Generate a failed job
    bad_table = f"non_existent_table_{uuid.uuid4().hex[:8]}"
    job_id = None
    
    try:
        client.sql(f"SELECT * FROM {bad_table}").collect()
    except Exception as e:
        # Extract job ID from error message if possible, or fetch recent jobs
        # Dremio error messages usually contain the job ID
        # But easier to just list recent jobs and find the failed one
        pass
        
    # 2. Find the failed job
    # We can use the agent's tool or client directly
    # Let's use the client to be sure
    try:
        # Try to get recent jobs via SQL
        jobs = client.sql("SELECT job_id, error_msg FROM sys.jobs WHERE status = 'FAILED' ORDER BY start_time DESC LIMIT 1").collect()
        if not jobs.empty:
            job_id = jobs['job_id'][0]
            error_msg = jobs['error_msg'][0]
            print(f"Found failed job: {job_id} with error: {error_msg}")
    except Exception:
        pytest.skip("Could not query sys.jobs to find failed job")

    if not job_id:
        pytest.skip("No failed job found to analyze")

    # 3. Analyze
    analysis = agent.analyze_job_failure(job_id)
    print(f"Agent Analysis: {analysis}")
    
    # 4. Verify
    assert analysis is not None
    assert len(analysis) > 10
    # The analysis should mention the table name or "not found"
    assert "not found" in analysis.lower() or bad_table.lower() in analysis.lower()

def test_list_recent_jobs(agent):
    """
    Test the list_recent_jobs tool via the agent.
    """
    # We can invoke the tool directly or ask the agent
    # Let's ask the agent to list jobs
    response = agent.agent.invoke({"messages": [("user", "List the 3 most recent jobs")]})
    output = response["messages"][-1].content
    print(f"Agent Output: {output}")
    
    assert output is not None
    # Should mention job IDs or status
    assert "job" in output.lower() or "status" in output.lower()
