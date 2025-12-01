# AI Observability Tools

The `DremioAgent` includes powerful observability tools to help you monitor and debug your Dremio environment.

## Features

### 1. Job Analysis
The agent can inspect job details, including status, duration, and error messages.

-   **Tool**: `get_job_details(job_id)`
-   **Tool**: `list_recent_jobs(limit)`

### 2. Failure Analysis
The agent can analyze failed jobs and provide actionable explanations and fixes using its LLM capabilities.

-   **Method**: `agent.analyze_job_failure(job_id)`

## Usage Examples

### Listing Recent Jobs

```python
from dremioframe.ai.agent import DremioAgent

agent = DremioAgent()

# Ask the agent to list jobs
response = agent.generate_script("List the 5 most recent failed jobs")
print(response)
```

### Analyzing a Failed Job

```python
# Analyze a specific job failure
job_id = "12345-67890-abcdef"
analysis = agent.analyze_job_failure(job_id)

print("Failure Analysis:")
print(analysis)
```

### Interactive Debugging

You can also use the agent to interactively debug issues:

```python
agent.agent.invoke({"messages": [("user", "Why did my last query fail?")]})
```
