# Space and Folder Management

DremioFrame provides methods to manage Spaces and Folders in both Dremio Cloud and Dremio Software.

## Methods

### `create_folder`

Creates a folder using SQL. This is the primary method for creating folders in Dremio Cloud and Iceberg Catalogs.

```python
client.admin.create_folder("my_space.my_folder")
```

**Syntax:** `CREATE FOLDER [IF NOT EXISTS] <folder_name>`

### `create_space`

Creates a Space using the REST API. This is specific to **Dremio Software**.

```python
client.admin.create_space("NewSpace")
```

### `create_space_folder`

Creates a folder within a Space using the REST API. This is specific to **Dremio Software**.

```python
client.admin.create_space_folder("NewSpace", "SubFolder")
```

## Differences between Cloud and Software

*   **Dremio Cloud**: Uses `create_folder` for all folder creation. Top-level folders act as spaces.
*   **Dremio Software**: Uses `create_space` for top-level containers (Spaces) and `create_space_folder` for folders within them. `create_folder` can also be used if the backend supports the SQL syntax (e.g. Nessie/Iceberg sources).
