# AI Script Generation

`dremioframe` includes an AI-powered module that can generate Python scripts for you based on natural language prompts. It uses LangChain and supports OpenAI, Anthropic, and Google Gemini models.

## Installation

To use the AI features, you must install the optional `ai` dependencies:

```bash
pip install dremioframe[ai]
```

## Configuration

You need to set the API key for your chosen model provider in your environment variables:

- **OpenAI**: `OPENAI_API_KEY`
- **Anthropic**: `ANTHROPIC_API_KEY`
- **Google Gemini**: `GOOGLE_API_KEY`

## Usage via CLI

You can generate scripts directly from the command line using the `generate` command.

```bash
# Generate a script to list all sources
dremio-cli generate "List all sources and print their names" --output list_sources.py

# Use a specific model (default is gpt-4o)
dremio-cli generate "Create a view named 'sales_summary' from 'sales_raw'" --model claude-3-opus --output create_view.py

# Use a prompt from a file
dremio-cli generate prompt.txt --output script.py
```

## Usage via Python

You can also use the `DremioAgent` class directly in your Python code.

```python
from dremioframe.ai.agent import DremioAgent

# Initialize the agent
agent = DremioAgent(model="gpt-4o")

# Generate a script
prompt = "Write a script to connect to Dremio and list all spaces."
script = agent.generate_script(prompt)

print(script)

# Save to file
agent.generate_script(prompt, output_file="list_spaces.py")
```

## How it Works

The agent has access to the `dremioframe` documentation and uses it to understand how to use the library. It generates a complete Python script that includes:

1.  Importing `DremioClient`.
2.  Initializing the client (expecting `DREMIO_PAT` and `DREMIO_PROJECT_ID` env vars).
3.  Performing the requested actions using the appropriate methods.
