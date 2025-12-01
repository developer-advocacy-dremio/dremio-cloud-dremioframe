# DremioAgent Class

The `DremioAgent` class is the core of the AI capabilities in `dremioframe`. It uses LangGraph and LangChain to create an agent that can understand your request, consult documentation, inspect the Dremio catalog, and generate code, SQL, or API calls.

## Purpose

The `DremioAgent` is designed to:
1.  **Generate Python Scripts**: Create complete, runnable scripts using `dremioframe` to automate tasks.
2.  **Generate SQL Queries**: Write complex SQL queries, validating table names and columns against the actual catalog.
3.  **Generate API Calls**: Construct cURL commands for the Dremio REST API by referencing the documentation.

## Constructor

```python
class DremioAgent(
    model: str = "gpt-4o", 
    api_key: Optional[str] = None, 
    llm: Optional[BaseChatModel] = None,
    memory_path: Optional[str] = None,
    context_folder: Optional[str] = None
)
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `model` | `str` | `"gpt-4o"` | The name of the LLM model to use. Supported providers are OpenAI, Anthropic, and Google. |
| `api_key` | `Optional[str]` | `None` | The API key for the chosen model provider. If not provided, the agent will look for the corresponding environment variable (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GOOGLE_API_KEY`). |
| `llm` | `Optional[BaseChatModel]` | `None` | A pre-configured LangChain Chat Model instance. If provided, `model` and `api_key` arguments are ignored. Use this to support other providers (AWS Bedrock, Ollama, Azure, etc.). |
| `memory_path` | `Optional[str]` | `None` | Path to a SQLite database file for persisting conversation history. If provided, conversations can be resumed using a `session_id`. Requires `langgraph-checkpoint-sqlite` (included in `ai` optional dependencies). |
| `context_folder` | `Optional[str]` | `None` | Path to a folder containing additional context files (schemas, documentation, etc.). The agent can list and read these files when generating code or SQL. |

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

## Memory Persistence

By default, the `DremioAgent` does not persist conversation history between sessions. Each invocation starts fresh. To enable memory persistence, provide a `memory_path` when creating the agent.

### Basic Usage

```python
from dremioframe.ai.agent import DremioAgent

# Create agent with memory
agent = DremioAgent(memory_path="./agent_memory.db")

# First conversation
code1 = agent.generate_script("Create a table from CSV", session_id="user123")

# Later, in the same or different session
code2 = agent.generate_script("Now add a column to that table", session_id="user123")
# The agent remembers the previous conversation and knows which table you're referring to
```

### How It Works

- **SQLite Database**: Conversation history is stored in a SQLite database at the specified path.
- **Session ID**: Each conversation thread is identified by a `session_id`. Use the same `session_id` to continue a conversation.
- **Thread Isolation**: Different `session_id` values create independent conversation threads.

### Example: Multi-Turn Conversation

```python
agent = DremioAgent(memory_path="./conversations.db")

session = "project_alpha"

# Turn 1
sql1 = agent.generate_sql("Get all users from the users table", session_id=session)
print(sql1)  # SELECT * FROM users

# Turn 2 - Agent remembers context
sql2 = agent.generate_sql("Filter to only active users", session_id=session)
print(sql2)  # SELECT * FROM users WHERE status = 'active'

# Turn 3 - Agent still has context
sql3 = agent.generate_sql("Add their email addresses", session_id=session)
print(sql3)  # SELECT *, email FROM users WHERE status = 'active'
```

### Managing Sessions

```python
# Different projects/users can have separate conversations
agent.generate_script("Create ETL pipeline", session_id="project_alpha")
agent.generate_script("Create dashboard", session_id="project_beta")

# Each session maintains its own context
```

### Clearing Memory

To clear all conversation history, simply delete the SQLite database file:

```python
import os
os.remove("./agent_memory.db")
```

## Context Folder

The `context_folder` feature allows the agent to access files from a specified directory. This is useful for:
- Reading project-specific schemas
- Referencing data dictionaries
- Using custom documentation
- Accessing configuration files

### Basic Usage

```python
from dremioframe.ai.agent import DremioAgent

# Create agent with context folder
agent = DremioAgent(context_folder="./project_docs")

# Agent can now reference files in ./project_docs
script = agent.generate_script(
    "Create a table based on the schema in schema.sql"
)
```

### Example: Project Documentation

```
project_docs/
├── schema.sql
├── data_dictionary.md
└── business_rules.txt
```

```python
agent = DremioAgent(context_folder="./project_docs")

# Agent can read these files when needed
sql = agent.generate_sql(
    "Create a query following the business rules in business_rules.txt"
)
```

### How It Works

When `context_folder` is set, the agent gains two additional tools:
1.  **`list_context_files()`**: Lists all files in the context folder.
2.  **`read_context_file(file_path)`**: Reads the content of a specific file.

The agent automatically uses these tools when your prompt mentions files or context.

### Example: Schema-Driven Development

```python
# schema.sql contains:
# CREATE TABLE customers (
#     id INT,
#     name VARCHAR(100),
#     email VARCHAR(100),
#     created_at TIMESTAMP
# )

agent = DremioAgent(context_folder="./schemas")

# Agent reads schema.sql and generates appropriate code
code = agent.generate_script(
    "Create a Python script to validate that the customers table matches the schema in schema.sql"
)
```

### Combining Memory and Context

```python
agent = DremioAgent(
    memory_path="./memory.db",
    context_folder="./project_docs"
)

session = "data_migration"

# Turn 1
agent.generate_script(
    "Read the source schema from source_schema.sql",
    session_id=session
)

# Turn 2 - Agent remembers the source schema from Turn 1
agent.generate_script(
    "Now generate a migration script to the target schema in target_schema.sql",
    session_id=session
)
```

## Best Practices

### Memory Persistence
- **Use descriptive session IDs**: `"user_123_project_alpha"` is better than `"session1"`.
- **Clean up old sessions**: Periodically delete the database or implement session expiration.
- **Don't share session IDs**: Each user or project should have unique session IDs.

### Context Folder
- **Keep files small**: Large files may exceed LLM context limits.
- **Use clear file names**: `customer_schema.sql` is better than `schema1.sql`.
- **Organize by topic**: Group related files in subdirectories.
- **Mention files explicitly**: "Use the schema in schema.sql" is clearer than "use the schema".

### Performance
- **Limit context folder size**: Too many files can slow down the agent.
- **Use memory sparingly**: Long conversation histories increase token usage.
- **Clear memory between projects**: Start fresh for unrelated work.
