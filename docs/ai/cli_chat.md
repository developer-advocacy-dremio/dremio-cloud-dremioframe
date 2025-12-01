# Interactive CLI Chat

The `dremioframe` CLI now includes an interactive chat mode, allowing you to converse with the Dremio Agent directly from your terminal.

## Features

-   **Natural Language Interface**: Ask questions, request scripts, or troubleshoot issues in plain English.
-   **Context Awareness**: The agent maintains conversation history, allowing for follow-up questions.
-   **Tool Access**: The agent can use all available tools (Catalog, Jobs, Reflections, etc.) during the chat.

## Usage

To start the chat session, run:

```bash
dremioframe chat
```

You can also specify the model (default is `gpt-4o`):

```bash
dremioframe chat --model claude-3-opus
```

## Example Session

```text
$ dremioframe chat
Starting Dremio Agent Chat (gpt-4o)...
Type 'exit' or 'quit' to leave.

You: List the most recent failed jobs.
Thinking...
Agent: Here are the 3 most recent failed jobs:
1. Job ID: 123... - Error: Table not found
2. Job ID: 456... - Error: Syntax error
...
----------------------------------------
You: Analyze the first one.
Thinking...
Agent: I've analyzed job 123... The error "Table not found" suggests that the table 'sales.data' does not exist. 
I checked the catalog and found 'sales.raw_data'. Did you mean that?
----------------------------------------
```
