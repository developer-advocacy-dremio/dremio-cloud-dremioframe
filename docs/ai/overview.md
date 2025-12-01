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

## Getting Started

To use the AI features, you need to install the optional dependencies:

```bash
pip install dremioframe[ai]
```

You also need to set your LLM API key in your environment variables (e.g., `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, or `GOOGLE_API_KEY`).

## Usage

You can use the agent programmatically in your Python scripts or interactively via the CLI.

```python
from dremioframe.ai.agent import DremioAgent

agent = DremioAgent(model="gpt-4o")
response = agent.generate_script("List all spaces")
print(response)
```
