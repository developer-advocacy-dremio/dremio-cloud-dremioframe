# AI SQL Generation

The `generate-sql` command (or `agent.generate_sql()` method) generates a SQL query based on a natural language prompt. It uses the `list_catalog_items` and `get_table_schema` tools to inspect your Dremio catalog and validate table names and columns.

## Usage via CLI

The `generate-sql` command takes the following arguments:

- `prompt` (Required): The natural language description of the SQL query you want to generate.
- `--model` / `-m` (Optional): The LLM model to use. Defaults to `gpt-4o`.

**Examples:**

```bash
# Generate a query to select data from a specific table (using default gpt-4o)
dremio-cli generate-sql "Select the first 10 rows from the zips table in Samples"

# Generate a complex aggregation using Claude 3 Opus
dremio-cli generate-sql "Calculate the average population by state from the zips table" --model claude-3-opus
```

## Usage via Python

```python
from dremioframe.ai.agent import DremioAgent

agent = DremioAgent()
sql = agent.generate_sql("Select the first 10 rows from the zips table in Samples")
print(sql)
```

## How it Works

1.  **Context Awareness**: The agent is aware of your Dremio environment's structure (via catalog tools).
2.  **Validation**: It attempts to verify table names and columns against the actual catalog to prevent errors.
3.  **Security**: The agent generates the SQL but does not execute it automatically. You have full control to review and run the output.
