# Future Recommendations for DremioFrame

Based on the current state of the library, here are recommended enhancements to take `dremioframe` to the next level.

## 1. Enhanced Arrow Flight Configuration
Currently, `DremioClient` assumes the Flight endpoint shares the hostname with the REST API and defaults to port 443 or 9047.
**Recommendation**:
-   Add specific `flight_port` and `flight_endpoint` arguments to `DremioClient`.
-   Support advanced Flight authentication mechanisms if needed.

## 2. dbt Integration
Many data teams use dbt for transformation.
**Recommendation**:
-   Create a `DbtTask` in the orchestration module.
-   This task would wrap `dbt run`, `dbt test`, etc., allowing users to mix Python tasks and dbt models in the same pipeline.

## 3. Orchestration Sensors
Add "Sensor" tasks that wait for a condition to be met before proceeding.
**Recommendation**:
-   `SqlSensor`: Polls a SQL query until it returns a specific value (e.g., "SELECT count(*) > 0 FROM staging").
-   `FileSensor`: Checks for the existence of a file in a Dremio source (via `LIST_FILES`).

## 4. SCD2 Helper
Slowly Changing Dimension Type 2 (SCD2) is a common but complex pattern for tracking history.
**Recommendation**:
-   Add `builder.scd2(target, on, ...)` method.
-   This would automate the logic of closing old records (updating `valid_to`) and inserting new records (`valid_from`).

## 5. Interactive CLI (REPL)
The current CLI is basic.
**Recommendation**:
-   Build a rich interactive shell (using `prompt_toolkit` or `rich`).
-   Features: Syntax highlighting, auto-completion for table names, and pretty-printed results.

## 6. Pydantic Schema Integration
While `create` and `insert` support Pydantic, deeper integration would be powerful.
**Recommendation**:
-   Add `builder.validate(schema)`: Runs a query to check if existing data conforms to the schema (checking types, nulls, constraints).
-   Generate Dremio `CREATE TABLE` DDL directly from Pydantic models.
