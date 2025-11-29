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
