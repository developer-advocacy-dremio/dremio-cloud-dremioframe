# Slowly Changing Dimensions (SCD2) Guide

This guide explains how to use the `scd2` method in DremioFrame to manage Type 2 Slowly Changing Dimensions.

## What is SCD2?

Type 2 Slowly Changing Dimension (SCD2) is a technique to track historical data by creating multiple records for a given natural key, each representing a specific time range. This allows you to query the state of a record at any point in the past.

## Table Design

To use the `scd2` helper, your target table must be designed with two special columns to track the validity period of each record:

- **`valid_from`** (TIMESTAMP): The time when the record became active.
- **`valid_to`** (TIMESTAMP): The time when the record ceased to be active. `NULL` indicates the current active record.

### Example Schema

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | INTEGER | Natural Key (Unique ID of the entity) |
| `name` | VARCHAR | Attribute to track |
| `status` | VARCHAR | Attribute to track |
| `valid_from` | TIMESTAMP | Start of validity |
| `valid_to` | TIMESTAMP | End of validity (NULL for active) |

## How it Works

The `scd2` method performs two operations in a single call:

1.  **Close Old Records**: It identifies records in the target table that have changed in the source. It updates their `valid_to` column to the current timestamp.
2.  **Insert New Records**: It inserts new versions of the changed records (and completely new records) from the source into the target. These new records have `valid_from` set to the current timestamp and `valid_to` set to `NULL`.

## Usage

```python
from dremioframe.client import DremioClient

client = DremioClient(...)

# Define your source (e.g., a staging table or view)
source = client.table("staging.customers")

# Apply SCD2 logic to the target dimension table
source.scd2(
    target_table="warehouse.dim_customers",
    on=["id"],                      # Natural key(s) to join on
    track_cols=["name", "status"],  # Columns to check for changes
    valid_from_col="valid_from",    # Name of your valid_from column
    valid_to_col="valid_to"         # Name of your valid_to column
)
```

## Before and After Example

### Initial State (Target Table)

| id | name | status | valid_from | valid_to |
| :--- | :--- | :--- | :--- | :--- |
| 1 | Alice | Active | 2023-01-01 10:00:00 | NULL |
| 2 | Bob | Active | 2023-01-01 10:00:00 | NULL |

### Source Data (New Batch)

| id | name | status |
| :--- | :--- | :--- |
| 1 | Alice | Inactive |  <-- Changed Status
| 2 | Bob | Active |      <-- No Change
| 3 | Charlie | Active |  <-- New Record

### After `scd2` Execution

| id | name | status | valid_from | valid_to | Note |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | Alice | Active | 2023-01-01 10:00:00 | **2023-01-02 12:00:00** | Closed (Old Version) |
| 1 | Alice | Inactive | **2023-01-02 12:00:00** | NULL | **New Version** |
| 2 | Bob | Active | 2023-01-01 10:00:00 | NULL | Unchanged |
| 3 | Charlie | Active | **2023-01-02 12:00:00** | NULL | **New Record** |

## Logic Breakdown

1.  **Identify Changes**: The method joins the source and target on `id`. It compares `name` and `status`.
2.  **Update**: For ID 1, `status` changed. The old record (where `valid_to` is NULL) is updated with `valid_to = NOW()`.
3.  **Insert**:
    -   ID 1 (New Version): Inserted with `valid_from = NOW()`, `valid_to = NULL`.
    -   ID 3 (New Record): Inserted with `valid_from = NOW()`, `valid_to = NULL`.
    -   ID 2: Ignored because it matched and no columns changed.
