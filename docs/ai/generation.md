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

### Arguments for `generate_script`

- `prompt` (str): The natural language request describing the script you want to generate.
- `output_file` (Optional[str]): The path to save the generated script to. If not provided, the script is returned as a string.

### Using Custom LLMs

You can use any LangChain-compatible chat model by passing it to the `DremioAgent` constructor.

```python
from dremioframe.ai.agent import DremioAgent
from langchain_openai import ChatOpenAI

# Initialize with a custom LLM instance
custom_llm = ChatOpenAI(model="gpt-4-turbo", temperature=0.5)
agent = DremioAgent(llm=custom_llm)

# Generate a script
prompt = "Write a script to connect to Dremio and list all spaces."
script = agent.generate_script(prompt)

print(script)
```

### Example: Using a Local LLM (e.g., Ollama)

You can use a local LLM running via Ollama or any other OpenAI-compatible server.

```python
from dremioframe.ai.agent import DremioAgent
from langchain_openai import ChatOpenAI

# Connect to local Ollama instance
local_llm = ChatOpenAI(
    base_url="http://localhost:11434/v1",
    api_key="ollama", # Required but ignored
    model="llama3"
)

agent = DremioAgent(llm=local_llm)
agent.generate_script("List all sources")
```

### Example: Using Amazon Bedrock

To use Amazon Bedrock, you need to install `langchain-aws`.

```bash
pip install langchain-aws
```

```python
from dremioframe.ai.agent import DremioAgent
from langchain_aws import ChatBedrock

# Initialize Bedrock client
bedrock_llm = ChatBedrock(
    model_id="anthropic.claude-3-sonnet-20240229-v1:0",
    model_kwargs={"temperature": 0.1}
)

agent = DremioAgent(llm=bedrock_llm)
agent.generate_script("Create a view from sales data")
```

## How it Works

The agent has access to:
1.  **Library Documentation**: It can list and read `dremioframe` documentation files.
2.  **Dremio Documentation**: It can search and read native Dremio documentation (if available in `dremiodocs/`) to understand SQL functions and concepts.

It generates a complete Python script that includes:
1.  Importing `DremioClient`.
2.  Initializing the client (expecting `DREMIO_PAT` and `DREMIO_PROJECT_ID` env vars).
3.  Performing the requested actions using the appropriate methods.
