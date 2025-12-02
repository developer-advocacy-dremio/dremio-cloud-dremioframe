# Document Extraction to Tables

The DremioAgent can extract structured data from documents (PDFs, markdown files) in your context folder and generate code to create or insert data into Dremio tables.

## Installation

```bash
pip install dremioframe[document]
```

## Usage

### Basic Workflow

1. Place documents in a context folder
2. Initialize agent with context folder
3. Ask agent to extract data with a specific schema
4. Agent generates code to create/insert into Dremio

### Example: Extract Invoice Data from PDFs

```python
from dremioframe.ai.agent import DremioAgent

# Initialize agent with context folder
agent = DremioAgent(
    model="gpt-4o",
    context_folder="./invoices"
)

# Ask agent to extract structured data
script = agent.generate_script("""
Read all PDF files in the context folder that contain invoice data.
Extract the following fields: invoice_number, date, customer_name, total_amount.
Generate code to create a table 'invoices' with this schema and insert the extracted data.
""")

print(script)
```

### Example: Extract Product Catalog from Markdown

```python
agent = DremioAgent(
    model="gpt-4o",
    context_folder="./product_docs"
)

script = agent.generate_script("""
Read all markdown files in the context folder.
Extract product information: product_id, name, category, price, description.
Create a table 'products' and insert the data.
""")
```

## Supported Document Types

### PDF Files
- Uses `pdfplumber` for text extraction
- Works with text-based PDFs
- Scanned PDFs require OCR (not currently supported)

### Markdown Files
- Direct text reading
- Supports tables, lists, and structured content

### Text Files
- Any `.txt`, `.md`, `.csv` files
- Read via `read_context_file` tool

## How It Works

1. **List Files**: Agent uses `list_context_files()` to see available documents
2. **Read Content**: Agent uses `read_pdf_file()` or `read_context_file()` to extract text
3. **Extract Data**: LLM analyzes content and extracts structured data
4. **Generate Code**: Agent creates Python code to load data into Dremio

## Best Practices

### Provide Clear Schema
Be specific about the fields you want extracted:
```python
"""
Extract these exact fields:
- invoice_number (string)
- date (YYYY-MM-DD format)
- customer_name (string)
- total_amount (decimal)
"""
```

### Use Examples
Provide example data if documents have varying formats:
```python
"""
Example invoice format:
Invoice #: INV-12345
Date: 2024-01-15
Customer: Acme Corp
Total: $1,500.00
"""
```

### Handle Missing Data
Specify how to handle missing fields:
```python
"""
If a field is missing, use NULL or empty string.
"""
```

## Limitations

- **OCR Not Supported**: Scanned PDFs won't work (text-based only)
- **Token Limits**: Very large documents may exceed LLM context windows
- **Accuracy**: Extraction quality depends on document structure and LLM capabilities
- **Performance**: Large batches of documents may be slow

## Future Enhancements

- Image OCR support (via Tesseract or cloud services)
- Table extraction from PDFs
- Multi-page document handling
- Batch processing optimization
