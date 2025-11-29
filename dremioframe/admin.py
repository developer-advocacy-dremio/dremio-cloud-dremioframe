from typing import Optional, List, Any

class Admin:
    def __init__(self, client):
        self.client = client

    # --- Users ---
    def create_user(self, username: str, password: Optional[str] = None):
        """Create a new user."""
        sql = f"CREATE USER '{username}'"
        if password:
            sql += f" PASSWORD '{password}'"
        return self.client.execute(sql)

    def alter_user_password(self, username: str, password: str):
        """Change a user's password."""
        return self.client.execute(f"ALTER USER '{username}' SET PASSWORD '{password}'")

    def drop_user(self, username: str):
        """Delete a user."""
        return self.client.execute(f"DROP USER '{username}'")

    # --- Grants ---
    def grant(self, privilege: str, on: str, to_user: str = None, to_role: str = None):
        """
        Grant privileges.
        Example: grant("SELECT", "TABLE my_table", to_role="DATA_SCIENTIST")
        """
        if to_user:
            grantee = f"USER '{to_user}'"
        elif to_role:
            grantee = f"ROLE '{to_role}'"
        else:
            raise ValueError("Must specify to_user or to_role")
            
        return self.client.execute(f"GRANT {privilege} ON {on} TO {grantee}")

    def revoke(self, privilege: str, on: str, from_user: str = None, from_role: str = None):
        """
        Revoke privileges.
        """
        if from_user:
            grantee = f"USER '{from_user}'"
        elif from_role:
            grantee = f"ROLE '{from_role}'"
        else:
            raise ValueError("Must specify from_user or from_role")
            
        return self.client.execute(f"REVOKE {privilege} ON {on} FROM {grantee}")

    # --- Policies ---
    def create_policy_function(self, name: str, args: str, return_type: str, body: str):
        """
        Create a UDF to be used as a policy.
        Example: create_policy_function("mask_ssn", "ssn VARCHAR", "VARCHAR", "CASE WHEN is_member('admin') THEN ssn ELSE '***' END")
        """
        return self.client.execute(f"CREATE FUNCTION {name} ({args}) RETURNS {return_type} RETURN {body}")

    def apply_masking_policy(self, table: str, column: str, policy: str):
        """
        Apply a column masking policy.
        policy: Function name and args, e.g. "mask_ssn(ssn)"
        """
        return self.client.execute(f"ALTER TABLE {table} MODIFY COLUMN {column} SET MASKING POLICY {policy}")

    def drop_masking_policy(self, table: str, column: str):
        """Remove a masking policy from a column."""
        return self.client.execute(f"ALTER TABLE {table} MODIFY COLUMN {column} UNSET MASKING POLICY")

    def apply_row_access_policy(self, table: str, policy: str):
        """
        Apply a row access policy.
        policy: Function name and args, e.g. "region_filter(region)"
        """
        return self.client.execute(f"ALTER TABLE {table} ADD ROW ACCESS POLICY {policy}")

    def drop_row_access_policy(self, table: str):
        """Remove a row access policy."""
        return self.client.execute(f"ALTER TABLE {table} DROP ROW ACCESS POLICY")

    # --- Reflections ---
    def list_reflections(self):
        """List all reflections."""
        # This requires using the REST API directly as there is no SQL command for listing reflections in this detail
        # Dremio REST API: GET /api/v3/reflection
        response = self.client.session.get(f"{self.client.base_url}/reflection")
        response.raise_for_status()
        return response.json()

    def create_reflection(self, dataset_id: str, name: str, type: str, 
                          display_fields: List[str] = None, 
                          dimension_fields: List[str] = None, 
                          measure_fields: List[str] = None,
                          distribution_fields: List[str] = None,
                          partition_fields: List[str] = None,
                          sort_fields: List[str] = None):
        """
        Create a reflection.
        
        Args:
            dataset_id: The ID of the dataset.
            name: Name of the reflection.
            type: "RAW" or "AGGREGATION".
            display_fields: List of fields to display (RAW only).
            dimension_fields: List of dimension fields (AGGREGATION only).
            measure_fields: List of measure fields (AGGREGATION only).
            distribution_fields: List of fields to distribute by.
            partition_fields: List of fields to partition by.
            sort_fields: List of fields to sort by.
        """
        payload = {
            "name": name,
            "type": type.upper(),
            "datasetId": dataset_id,
            "enabled": True
        }
        
        if display_fields: payload["displayFields"] = [{"name": f} for f in display_fields]
        if dimension_fields: payload["dimensionFields"] = [{"name": f} for f in dimension_fields]
        if measure_fields: payload["measureFields"] = [{"name": f} for f in measure_fields]
        if distribution_fields: payload["distributionFields"] = [{"name": f} for f in distribution_fields]
        if partition_fields: payload["partitionFields"] = [{"name": f} for f in partition_fields]
        if sort_fields: payload["sortFields"] = [{"name": f} for f in sort_fields]

        response = self.client.session.post(f"{self.client.base_url}/reflection", json=payload)
        response.raise_for_status()
        return response.json()

    def delete_reflection(self, reflection_id: str):
        """Delete a reflection by ID."""
        response = self.client.session.delete(f"{self.client.base_url}/reflection/{reflection_id}")
        response.raise_for_status()
        return True

    def enable_reflection(self, reflection_id: str):
        """Enable a reflection."""
        # First get the reflection to get the tag (version)
        ref = self._get_reflection(reflection_id)
        ref["enabled"] = True
        return self._update_reflection(reflection_id, ref)

    def disable_reflection(self, reflection_id: str):
        """Disable a reflection."""
        ref = self._get_reflection(reflection_id)
        ref["enabled"] = False
        return self._update_reflection(reflection_id, ref)

    def _get_reflection(self, reflection_id: str):
        response = self.client.session.get(f"{self.client.base_url}/reflection/{reflection_id}")
        response.raise_for_status()
        return response.json()

    def _update_reflection(self, reflection_id: str, payload: dict):
        response = self.client.session.put(f"{self.client.base_url}/reflection/{reflection_id}", json=payload)
        response.raise_for_status()
        return response.json()
