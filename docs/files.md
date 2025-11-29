# Working with Files

Dremio allows you to query unstructured data (files) directly using the `LIST_FILES` table function. DremioFrame provides a helper method for this.

## Usage

```python
# List files in a folder
df = client.list_files("@my_user/documents")
df.select("file_path", "file_size").show()
```

This generates SQL:
```sql
SELECT file_path, file_size FROM TABLE(LIST_FILES('@my_user/documents'))
```

## Integrating with AI Functions

`LIST_FILES` is powerful when combined with AI functions for processing unstructured text (RAG - Retrieval Augmented Generation).

```python
from dremioframe import F

# 1. List PDF files
files = client.list_files("@my_source/contracts") \
    .filter("file_name LIKE '%.pdf'")

# 2. Use AI_GENERATE to extract data from files
# Note: We pass the 'file_content' column (implied reference) to the AI function
# The actual column name from LIST_FILES usually includes a 'file' struct or similar depending on Dremio version/source.
# Assuming 'file_path' is available or we pass the file reference.

# Example: Extracting entities from files
df = files.select(
    F.col("file_path"),
    F.ai_generate(
        "Extract parties and dates", 
        F.col("file_content") # Hypothetical column, check your source schema
    ).alias("extracted_info")
)
```
