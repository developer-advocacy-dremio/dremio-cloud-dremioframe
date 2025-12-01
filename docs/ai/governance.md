# AI Governance Tools

The `DremioAgent` assists with governance tasks such as auditing access and automating documentation.

## Features

### 1. Access Auditing
The agent can list privileges granted on specific entities (tables, views, folders, spaces).

-   **Tool**: `show_grants(entity)`

### 2. Automated Documentation
The agent can inspect a dataset's schema and automatically generate a Wiki description and suggest Tags.

-   **Method**: `agent.auto_document_dataset(path)`

## Usage Examples

### Auditing Access

```python
from dremioframe.ai.agent import DremioAgent

agent = DremioAgent()

# Check who has access to a sensitive table
response = agent.generate_script("Show grants for table 'finance.payroll'")
print(response)
```

### Auto-Documenting a Dataset

```python
# Generate documentation for a dataset
path = "sales.transactions"
documentation = agent.auto_document_dataset(path)

print("Generated Documentation:")
print(documentation)

# You can then use the client to apply this documentation
# import json
# doc_data = json.loads(documentation)
# client.catalog.update_wiki(dataset_id, doc_data['wiki'])
# client.catalog.set_tags(dataset_id, doc_data['tags'])
```
