from typing import Optional, List, Any, Union, Dict, Any
import pyarrow.flight as flight
import pandas as pd
import polars as pl
import os
from .quality import DataQuality

class DremioBuilder:
    def __init__(self, client, path: Optional[str] = None, sql: Optional[str] = None):
        self.client = client
        self.path = path
        self.initial_sql = sql
        self.select_columns = []
        self.mutations = {}
        self.filters = []
        self.limit_val = None
        self._quality = None

    @property
    def quality(self):
        if self._quality is None:
            self._quality = DataQuality(self)
        return self._quality
        
    def select(self, *columns: str) -> 'DremioBuilder':
        self.select_columns.extend(columns)
        return self

    def mutate(self, **kwargs) -> 'DremioBuilder':
        """
        Add calculated columns.
        Example: df.mutate(new_col="col1 + col2")
        """
        self.mutations.update(kwargs)
        return self

    def filter(self, condition: str) -> 'DremioBuilder':
        self.filters.append(condition)
        return self

    def limit(self, n: int) -> 'DremioBuilder':
        self.limit_val = n
        return self

    def _compile_sql(self) -> str:
        if self.initial_sql:
            # If started with raw SQL, we wrap it in a subquery to apply further operations
            query = f"SELECT * FROM ({self.initial_sql}) AS sub"
        else:
            # Quote the path components if needed, or assume user passed a valid path string
            # Dremio paths are usually "Space"."Folder"."Table"
            query = f"SELECT * FROM {self.path}"

        if self.select_columns or self.mutations:
            cols = []
            if self.select_columns:
                cols.extend(self.select_columns)
            
            if self.mutations:
                for name, expr in self.mutations.items():
                    cols.append(f"{expr} AS {name}")
            
            cols_str = ", ".join(cols)
            # Replace * with specific columns
            query = query.replace("SELECT *", f"SELECT {cols_str}", 1)

        if self.filters:
            where_clause = " AND ".join(self.filters)
            query += f" WHERE {where_clause}"

        if self.limit_val is not None:
            query += f" LIMIT {self.limit_val}"

        return query

    def collect(self, library: str = "polars") -> Union[pl.DataFrame, pd.DataFrame]:
        sql = self._compile_sql()
        return self._execute_flight(sql, library)

    def _execute_flight(self, sql: str, library: str) -> Union[pl.DataFrame, pd.DataFrame]:
        # Dremio Cloud Flight Endpoint
        location = "grpc+tls://data.dremio.cloud:443"
        
        headers = [
            (b"authorization", f"Bearer {self.client.pat}".encode("utf-8"))
        ]
        options = flight.FlightCallOptions(headers=headers)
        
        client = flight.FlightClient(location)
        info = client.get_flight_info(flight.FlightDescriptor.for_command(sql), options)
        reader = client.do_get(info.endpoints[0].ticket, options)
        table = reader.read_all()
        
        if library == "polars":
            return pl.from_arrow(table)
        elif library == "pandas":
            return table.to_pandas()
        else:
            raise ValueError("Library must be 'polars' or 'pandas'")

    def show(self, n: int = 20):
        print(self.limit(n).collect())

    # DML Operations
    def create(self, name: str):
        """Create table as select (CTAS)"""
        sql = self._compile_sql()
        create_sql = f"CREATE TABLE {name} AS {sql}"
        return self._execute_dml(create_sql)

    def insert(self, table_name: str, data: Union[Any, None] = None, batch_size: Optional[int] = None):
        """
        Insert into table.
        If data is provided (Arrow Table or Pandas DataFrame), it generates a VALUES clause.
        Otherwise, it inserts from the current selection.
        
        Args:
            table_name: Target table name.
            data: Optional PyArrow Table or Pandas DataFrame.
            batch_size: Optional integer to split data into batches.
        """
        if data is not None:
            # Handle Arrow Table or Pandas DataFrame
            import pyarrow as pa
            import math
            
            if isinstance(data, pd.DataFrame):
                data = pa.Table.from_pandas(data)
            
            if not isinstance(data, pa.Table):
                raise ValueError("Data must be a PyArrow Table or Pandas DataFrame")
            
            # Convert to VALUES clause
            rows = data.to_pylist()
            total_rows = len(rows)
            
            if batch_size is None:
                batch_size = total_rows
            
            num_batches = math.ceil(total_rows / batch_size)
            results = []
            
            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, total_rows)
                batch_rows = rows[start_idx:end_idx]
                
                values_list = []
                for row in batch_rows:
                    row_vals = []
                    for val in row.values():
                        if isinstance(val, str):
                            row_vals.append(f"'{val}'")
                        elif val is None:
                            row_vals.append("NULL")
                        else:
                            row_vals.append(str(val))
                    values_list.append(f"({', '.join(row_vals)})")
                
                values_clause = ", ".join(values_list)
                insert_sql = f"INSERT INTO {table_name} ({', '.join(data.column_names)}) VALUES {values_clause}"
                results.append(self._execute_dml(insert_sql))
                
            return results if len(results) > 1 else results[0]

        sql = self._compile_sql()
        insert_sql = f"INSERT INTO {table_name} {sql}"
        return self._execute_dml(insert_sql)

    def merge(self, target_table: str, on: Union[str, List[str]], 
              matched_update: Optional[Dict[str, str]] = None, 
              not_matched_insert: Optional[Dict[str, str]] = None,
              data: Union[Any, None] = None,
              batch_size: Optional[int] = None):
        """
        Perform a MERGE INTO operation (Upsert).
        
        Args:
            target_table: The table to merge into.
            on: Column(s) to join on.
            matched_update: Dict of {col: expr} to update when matched.
            not_matched_insert: Dict of {col: expr} to insert when not matched.
            data: Optional PyArrow Table or Pandas DataFrame to merge from.
            batch_size: Optional batch size for data.
        """
        if data is not None:
            # Similar to insert, handle data chunks
            import pyarrow as pa
            import math
            
            if isinstance(data, pd.DataFrame):
                data = pa.Table.from_pandas(data)
            
            rows = data.to_pylist()
            total_rows = len(rows)
            
            if batch_size is None:
                batch_size = total_rows
            
            num_batches = math.ceil(total_rows / batch_size)
            results = []
            
            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, total_rows)
                batch_rows = rows[start_idx:end_idx]
                
                # Create a VALUES clause for the source
                values_list = []
                for row in batch_rows:
                    row_vals = []
                    for val in row.values():
                        if isinstance(val, str):
                            row_vals.append(f"'{val}'")
                        elif val is None:
                            row_vals.append("NULL")
                        else:
                            row_vals.append(str(val))
                    values_list.append(f"({', '.join(row_vals)})")
                
                values_clause = ", ".join(values_list)
                source_alias = "source"
                # Construct the source subquery
                # We need to cast columns if necessary, but for now assume simple values
                # Dremio VALUES syntax: (val1, val2), ...
                # But we need to alias columns: SELECT * FROM (VALUES ...) AS source(col1, col2)
                cols_def = ", ".join(data.column_names)
                source_sql = f"(VALUES {values_clause}) AS {source_alias}({cols_def})"
                
                results.append(self._compile_and_run_merge(target_table, source_sql, on, matched_update, not_matched_insert))
            
            return results if len(results) > 1 else results[0]
        else:
            # Use current builder as source
            source_sql = f"({self._compile_sql()}) AS source"
            return self._compile_and_run_merge(target_table, source_sql, on, matched_update, not_matched_insert)

    def _compile_and_run_merge(self, target_table, source_sql, on, matched_update, not_matched_insert):
        if isinstance(on, str):
            on = [on]
        
        on_clause = " AND ".join([f"{target_table}.{col} = source.{col}" for col in on])
        
        parts = [f"MERGE INTO {target_table} USING {source_sql} ON ({on_clause})"]
        
        if matched_update:
            updates = ", ".join([f"{col} = {expr}" for col, expr in matched_update.items()])
            parts.append(f"WHEN MATCHED THEN UPDATE SET {updates}")
            
        if not_matched_insert:
            cols = ", ".join(not_matched_insert.keys())
            vals = ", ".join(not_matched_insert.values())
            parts.append(f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})")
            
        merge_sql = " ".join(parts)
        return self._execute_dml(merge_sql)

    def delete(self):
        """Delete from table based on filters"""
        if not self.path:
            raise ValueError("Delete requires a table path defined in constructor")
        
        if not self.filters:
            raise ValueError("Delete requires at least one filter to prevent accidental data loss")

        where_clause = " AND ".join(self.filters)
        delete_sql = f"DELETE FROM {self.path} WHERE {where_clause}"
        return self._execute_dml(delete_sql)

    def update(self, updates: dict):
        """Update table based on filters"""
        if not self.path:
            raise ValueError("Update requires a table path defined in constructor")
        
        if not self.filters:
            raise ValueError("Update requires at least one filter")

        set_clause = ", ".join([f"{k} = {v}" for k, v in updates.items()])
        where_clause = " AND ".join(self.filters)
        update_sql = f"UPDATE {self.path} SET {set_clause} WHERE {where_clause}"
        return self._execute_dml(update_sql)

    def _execute_dml(self, sql: str):
        # DML can be executed via Flight or REST. Flight is usually for large result sets.
        # For DML, we might want to use the REST API SQL endpoint if it exists, or just Flight.
        # Flight is fine for DML too, it just returns a result with affected rows.
        return self._execute_flight(sql, "polars")
