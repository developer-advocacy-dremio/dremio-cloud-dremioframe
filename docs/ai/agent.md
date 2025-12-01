# DremioAgent Class

The `DremioAgent` class is the core of the AI capabilities in `dremioframe`. It uses LangGraph and LangChain to create an agent that can understand your request, consult documentation, inspect the Dremio catalog, and generate code, SQL, or API calls.

## Purpose

The `DremioAgent` is designed to:
1.  **Generate Python Scripts**: Create complete, runnable scripts using `dremioframe` to automate tasks.
2.  **Generate SQL Queries**: Write complex SQL queries, validating table names and columns against the actual catalog.
3.  **Generate API Calls**: Construct cURL commands for the Dremio REST API by referencing the documentation.

## Constructor

```python
class DremioAgent(model: str = "gpt-4o", api_key: Optional[str] = None, llm: Optional[BaseChatModel] = None)
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `model` | `str` | `"gpt-4o"` | The name of the LLM model to use. Supported providers are OpenAI, Anthropic, and Google. |
| `api_key` | `Optional[str]` | `None` | The API key for the chosen model provider. If not provided, the agent will look for the corresponding environment variable (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GOOGLE_API_KEY`). |
| `llm` | `Optional[BaseChatModel]` | `None` | A pre-configured LangChain Chat Model instance. If provided, `model` and `api_key` arguments are ignored. Use this to support other providers (AWS Bedrock, Ollama, Azure, etc.). |

## Supported Models

The `model` argument supports string identifiers for major providers. The agent automatically selects the correct LangChain class based on the string.

### OpenAI
*Requires `OPENAI_API_KEY`*
- `gpt-4o` (Default)
- `gpt-4-turbo`
- `gpt-3.5-turbo`

### Anthropic
*Requires `ANTHROPIC_API_KEY`*
- `claude-3-opus-20240229`
- `claude-3-sonnet-20240229`
- `claude-3-haiku-20240307`

### Google Gemini
*Requires `GOOGLE_API_KEY`*
- `gemini-1.5-pro`
- `gemini-pro`

## Custom LLMs

To use a model provider not natively supported by the string shortcuts (like AWS Bedrock, Ollama, or Azure OpenAI), you can instantiate the LangChain model object yourself and pass it to the `llm` argument.

### Example: Local LLM (Ollama)

Use `ChatOpenAI` pointing to a local server (e.g., Ollama running Llama 3).

```python
from dremioframe.ai.agent import DremioAgent
from langchain_openai import ChatOpenAI

# Connect to local Ollama instance
local_llm = ChatOpenAI(
    base_url="http://localhost:11434/v1",
    api_key="ollama", # Required but ignored by Ollama
    model="llama3",
    temperature=0
)

agent = DremioAgent(llm=local_llm)
print(agent.generate_sql("List all tables in the 'Samples' space"))
```

### Example: AWS Bedrock

Use `ChatBedrock` from `langchain-aws`.

```bash
pip install langchain-aws
```

```python
from dremioframe.ai.agent import DremioAgent
from langchain_aws import ChatBedrock

bedrock_llm = ChatBedrock(
    model_id="anthropic.claude-3-sonnet-20240229-v1:0",
    model_kwargs={"temperature": 0}
)

agent = DremioAgent(llm=bedrock_llm)
```

### Example: Azure OpenAI

Use `AzureChatOpenAI` from `langchain-openai`.

```python
from dremioframe.ai.agent import DremioAgent
from langchain_openai import AzureChatOpenAI

azure_llm = AzureChatOpenAI(
    azure_deployment="my-gpt-4-deployment",
    openai_api_version="2023-05-15",
    temperature=0
)

agent = DremioAgent(llm=azure_llm)
```
