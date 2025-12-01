# Data Quality Recipes

This guide provides common patterns and recipes for validating your data using DremioFrame's Data Quality framework.

## Recipe 1: Validating Reference Data

Ensure that reference tables (like country codes or status lookups) contain expected values and no duplicates.

```yaml
- name: "Validate Country Codes"
  table: "reference.countries"
  checks:
    - type: "unique"
      column: "iso_code"
    
    - type: "not_null"
      column: "country_name"
      
    - type: "row_count"
      threshold: 190
      operator: "gt"
```

## Recipe 2: Financial Integrity Checks

Validate financial transactions for non-negative amounts and referential integrity (via custom SQL).

```yaml
- name: "Transaction Integrity"
  table: "finance.transactions"
  checks:
    - type: "custom_sql"
      condition: "MIN(amount) >= 0"
      error_msg: "Found negative transaction amounts"
      
    - type: "not_null"
      column: "transaction_date"
      
    # Check that all transactions belong to valid accounts (simplified referential integrity)
    - type: "custom_sql"
      condition: "(SELECT COUNT(*) FROM finance.transactions t LEFT JOIN finance.accounts a ON t.account_id = a.id WHERE a.id IS NULL) = 0"
      error_msg: "Found transactions for non-existent accounts"
```

## Recipe 3: Daily Ingestion Validation

Verify that a daily ingestion job loaded data correctly by checking row counts and freshness.

```yaml
- name: "Daily Web Logs"
  table: "raw.web_logs"
  checks:
    # Ensure we loaded at least some rows
    - type: "row_count"
      threshold: 0
      operator: "gt"
      
    # Ensure data is from today (assuming 'event_timestamp' column)
    - type: "custom_sql"
      condition: "MAX(event_timestamp) >= CURRENT_DATE"
      error_msg: "No data loaded for today"
```

## Recipe 4: Categorical Data Validation

Ensure columns with categorical data only contain allowed values.

```yaml
- name: "User Status Validation"
  table: "users.profiles"
  checks:
    - type: "values_in"
      column: "subscription_tier"
      values: ["free", "basic", "premium", "enterprise"]
      
    - type: "values_in"
      column: "email_verified"
      values: [true, false]
```

## Recipe 5: Running Tests Programmatically

You can run these YAML recipes from your Python code using the `DQRunner`.

```python
from dremioframe.client import DremioClient
from dremioframe.dq.runner import DQRunner

client = DremioClient()
runner = DQRunner(client)

# Load tests from a directory containing your YAML files
tests = runner.load_tests("./dq_checks")

# Run all loaded tests
success = runner.run_tests(tests)

if not success:
    print("Data Quality checks failed!")
    exit(1)
```
