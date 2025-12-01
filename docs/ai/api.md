# AI API Call Generation

The `generate-api` command (or `agent.generate_api_call()` method) generates a cURL command for the Dremio REST API. It uses the library's documentation and native Dremio documentation (if available) to find the correct endpoint and payload.

## Usage via CLI

The `generate-api` command takes the following arguments:

- `prompt` (Required): The natural language description of the API call you want to generate.
- `--model` / `-m` (Optional): The LLM model to use. Defaults to `gpt-4o`.

**Examples:**

```bash
# Generate a cURL command to list all sources (using default gpt-4o)
dremio-cli generate-api "List all sources"

# Generate a command to create a space using Claude 3 Opus
dremio-cli generate-api "Create a space named 'Marketing'" --model claude-3-opus

# Generate a command to trigger a reflection refresh
dremio-cli generate-api "Refresh the reflection with ID 12345"
```

## Usage via Python

```python
from dremioframe.ai.agent import DremioAgent

agent = DremioAgent()
curl = agent.generate_api_call("List all sources")
print(curl)
```

## How it Works

1.  **Context Awareness**: The agent is aware of the API specification (via documentation).
2.  **Security**: The agent generates the command but does not execute it automatically. You have full control to review and run the output.
