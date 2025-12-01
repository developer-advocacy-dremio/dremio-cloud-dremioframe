# AI Functions

Dremio provides AI-powered functions for classification, text completion, and structured data generation.

## Usage

```python
from dremioframe import F

# Classify
df.select(
    F.ai_classify(F.col("review"), ["Positive", "Negative"]).alias("sentiment")
)

# Complete
df.select(
    F.ai_complete("Summarize this text: " + F.col("text")).alias("summary")
)

# Generate Structured Data
df.select(
    F.ai_generate(
        "Extract entities", 
        schema="ROW(person VARCHAR, location VARCHAR)"
    ).alias("entities")
)
```

### Raw SQL Usage

You can also use AI functions by writing the SQL string directly in `mutate` or `select`.

```python
df.mutate(
    spice_level="AI_CLASSIFY('Identify the Spice Level:' || ARRAY_TO_STRING(ingredients, ','), ARRAY [ 'mild', 'medium', 'spicy' ])"
)
```

## Available Functions

| Function | Description |
| :--- | :--- |
| `ai_classify(prompt, categories, model_name=None)` | Classifies text into one of the provided categories. |
| `ai_complete(prompt, model_name=None)` | Generates a text completion for the prompt. |
| `ai_generate(prompt, model_name=None, schema=None)` | Generates structured data based on the prompt. Use `schema` to define the output structure (e.g., `ROW(...)`). |

### Examples

#### AI_CLASSIFY

```python
F.ai_classify("Is this email spam?", ["Spam", "Not Spam"])
F.ai_classify("Categorize product", ["Electronics", "Clothing"], model_name="gpt-4")
```

#### AI_COMPLETE

```python
F.ai_complete("Write a SQL query to find top users")
F.ai_complete("Translate to French", model_name="gpt-3.5")
```

#### AI_GENERATE

```python
# Generate structured data
F.ai_generate(
    "Extract customer info", 
    schema="ROW(name VARCHAR, age INT)"
)

# With specific model
F.ai_generate(
    "Extract info", 
    model_name="gpt-4", 
    schema="ROW(summary VARCHAR)"
)
```

## Using with LIST_FILES

You can combine AI functions with `client.list_files()` to process unstructured data.

```python
# Process all text files in a folder
client.list_files("@source/folder") \
    .filter("file_name LIKE '%.txt'") \
    .select(
        F.col("file_name"),
        F.ai_classify("Sentiment?", F.col("file_content"), ["Positive", "Negative"])
    )
```
