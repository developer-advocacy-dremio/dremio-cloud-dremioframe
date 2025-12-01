# Slowly Changing Dimensions (SCD)

Slowly Changing Dimensions (SCD) are techniques used in data warehousing to manage how data changes over time. DremioFrame provides support for the two most common types: Type 1 and Type 2.

## Type 1 (Overwrite)

SCD Type 1 overwrites the old data with the new data. No history is kept. This is useful for correcting errors or when historical values are not significant (e.g., correcting a spelling mistake in a name).

### Implementation

Use the `merge` method to perform an upsert (Update if exists, Insert if new).

```python
# Source data contains the latest state of users
new_user_data = client.table("staging.users")

# Target dimension table
target_table = "gold.dim_users"

# Perform Merge
client.table(target_table).merge(
    target_table=target_table,
    on="user_id",
    matched_update={
        "email": "source.email", 
        "status": "source.status",
        "updated_at": "CURRENT_TIMESTAMP"
    },
    not_matched_insert={
        "user_id": "source.user_id", 
        "email": "source.email", 
        "status": "source.status",
        "created_at": "CURRENT_TIMESTAMP",
        "updated_at": "CURRENT_TIMESTAMP"
    },
    data=new_user_data
)
```

## Type 2 (History)

SCD Type 2 tracks historical data by creating multiple records for a given natural key, each representing a specific time range. This allows you to query the state of a record at any point in the past.

### Table Design

To use SCD2, your target table must be designed with two special columns to track the validity period of each record:

- **`valid_from`** (TIMESTAMP): The time when the record became active.
- **`valid_to`** (TIMESTAMP): The time when the record ceased to be active. `NULL` indicates the current active record.

### Using the Helper

DremioFrame provides a `scd2` helper method to automate the complex logic of closing old records and inserting new ones.

```python
from dremioframe.client import DremioClient

client = DremioClient(...)

# Define your source (e.g., a staging table or view)
source = client.table("staging.customers")

# Apply SCD2 logic to the target dimension table
# This generates and executes the necessary SQL statements
source.scd2(
    target_table="warehouse.dim_customers",
    on=["id"],                      # Natural key(s) to join on
    track_cols=["name", "status"],  # Columns to check for changes
    valid_from_col="valid_from",    # Name of your valid_from column
    valid_to_col="valid_to"         # Name of your valid_to column
)
```

### Before and After Example

#### Initial State (Target Table)

| id | name | status | valid_from | valid_to |
| :--- | :--- | :--- | :--- | :--- |
| 1 | Alice | Active | 2023-01-01 10:00:00 | NULL |
| 2 | Bob | Active | 2023-01-01 10:00:00 | NULL |

#### Source Data (New Batch)

| id | name | status |
| :--- | :--- | :--- |
| 1 | Alice | Inactive |  <-- Changed Status
| 2 | Bob | Active |      <-- No Change
| 3 | Charlie | Active |  <-- New Record

#### After `scd2` Execution

| id | name | status | valid_from | valid_to | Note |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | Alice | Active | 2023-01-01 10:00:00 | **2023-01-02 12:00:00** | Closed (Old Version) |
| 1 | Alice | Inactive | **2023-01-02 12:00:00** | NULL | **New Version** |
| 2 | Bob | Active | 2023-01-01 10:00:00 | NULL | Unchanged |
| 3 | Charlie | Active | **2023-01-02 12:00:00** | NULL | **New Record** |

### Logic Breakdown

1.  **Identify Changes**: The method joins the source and target on `id`. It compares `name` and `status`.
2.  **Update**: For ID 1, `status` changed. The old record (where `valid_to` is NULL) is updated with `valid_to = NOW()`.
3.  **Insert**:
    -   ID 1 (New Version): Inserted with `valid_from = NOW()`, `valid_to = NULL`.
    -   ID 3 (New Record): Inserted with `valid_from = NOW()`, `valid_to = NULL`.
    -   ID 2: Ignored because it matched and no columns changed.
