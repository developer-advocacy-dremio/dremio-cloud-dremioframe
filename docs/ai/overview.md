# Dremio AI Agent

DremioFrame includes a powerful AI Agent powered by Large Language Models (LLMs) that acts as your intelligent co-pilot for Dremio development and administration.

## Capabilities

The AI Agent is designed to assist with:

-   **[Generation](generation.md)**: Generate Python Scripts, [SQL](sql.md) and [cURL](api.md) commands to interact with your Dremio instance
-   **[Observability](observability.md)**: Analyze job failures and monitor system health.
-   **[Reflections](reflections.md)**: Recommend and manage reflections for query acceleration.
-   **[Governance](governance.md)**: Audit access and automate dataset documentation.
-   **[Data Quality](data_quality.md)**: Generate data quality recipes automatically.
-   **[SQL Optimization](optimization.md)**: Analyze query plans and suggest performance improvements.
-   **[Interactive Chat](cli_chat.md)**: Converse with the agent directly from the CLI.

**note:** this libraries embdedded agent is primarily meant as a code generation assist tool, not meant as an alternative to the integrated Dremio agent for deeper administration and natural language analytics. Login to your Dremio instance's UI to leverage integrated agent.

## Getting Started

To use the AI features, you need to install the optional dependencies:

```bash
pip install dremioframe[ai]
```

You also need to set your LLM API key in your environment variables.

### Required Environment Variables

The AI agent supports multiple LLM providers. Set the appropriate environment variable for your chosen provider:

| Provider | Environment Variable | How to Get |
|----------|---------------------|------------|
| **OpenAI** | `OPENAI_API_KEY` | [platform.openai.com/api-keys](https://platform.openai.com/api-keys) |
| **Anthropic** | `ANTHROPIC_API_KEY` | [console.anthropic.com/settings/keys](https://console.anthropic.com/settings/keys) |
| **Google** | `GOOGLE_API_KEY` | [aistudio.google.com/app/apikey](https://aistudio.google.com/app/apikey) |

**Example `.env` file:**
```bash
# Choose one based on your provider
OPENAI_API_KEY=sk-proj-...
# ANTHROPIC_API_KEY=sk-ant-...
# GOOGLE_API_KEY=AIza...

# Dremio credentials (required for agent to access catalog)
DREMIO_PAT=your_personal_access_token
DREMIO_PROJECT_ID=your_project_id
DREMIO_URL=data.dremio.cloud
```

## Usage

You can use the agent programmatically in your Python scripts or interactively via the CLI.

```python
from dremioframe.ai.agent import DremioAgent

agent = DremioAgent(model="gpt-4o")
response = agent.generate_script("List all spaces")
print(response)
```
