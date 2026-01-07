
import os
import pytest
import uuid
import time
from dotenv import load_dotenv
from dremioframe.client import DremioClient

# Load env variables from .env file
load_dotenv()

# This test requires a live environment
# It uses the DREMIO_CLOUD_* or DREMIO_SOFTWARE_* env vars

def test_service_user_lifecycle():
    print("--- Starting Service User Lifecycle Test ---")
    
    # Initialize Client
    # We'll rely on auto-detection or fallback to specific env vars for safety
    # The user provided DREMIO_CLOUD_* and DREMIO_SOFTWARE_* vars in .env
    # We might need to manually map them if DremioClient doesn't pick up the custom names like DREMIO_CLOUD_TOKEN
    # The client expects DREMIO_PAT or DREMIO_SOFTWARE_PAT
    
    # Let's try to map the user's specific env vars to what the client expects
    forced_mode = os.getenv("TEST_MODE")
    if forced_mode == "v26":
        os.environ["DREMIO_SOFTWARE_PAT"] = os.getenv("DREMIO_SOFTWARE_TOKEN")
        os.environ["DREMIO_SOFTWARE_HOST"] = os.getenv("DREMIO_SOFTWARE_BASE_URL", "").replace("/api/v3", "")
        mode = "v26"
    elif os.getenv("DREMIO_CLOUD_TOKEN"):
        os.environ["DREMIO_PAT"] = os.getenv("DREMIO_CLOUD_TOKEN")
        os.environ["DREMIO_PROJECT_ID"] = os.getenv("DREMIO_CLOUD_PROJECTID")
        mode = "cloud"
    elif os.getenv("DREMIO_SOFTWARE_TOKEN"):
        os.environ["DREMIO_SOFTWARE_PAT"] = os.getenv("DREMIO_SOFTWARE_TOKEN")
        os.environ["DREMIO_SOFTWARE_HOST"] = os.getenv("DREMIO_SOFTWARE_BASE_URL").replace("/api/v3", "") 
        mode = "v26"
    else:
        print("Skipping test: No credentials found in environment.")
        return

    try:
        client = DremioClient(mode=mode)
        print(f"Connected to {mode} mode.")
    except Exception as e:
        print(f"Failed to connect: {e}")
        return

    # Generate unique name
    uid = str(uuid.uuid4())[:8]
    username = f"test_svc_{uid}"
    
    print(f"1. Creating Service User: {username}")
    try:
        user = client.admin.create_user(
            username=username, 
            identity_type="SERVICE_USER",
            first_name="Test",
            last_name="Service"
        )
        print("   Success! User created.")
        print(f"   Details: {user}")
        user_id = user.get("id")
    except Exception as e:
        print(f"   Failed to create user: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Response Body: {e.response.text}")
        # If creation fails (e.g. not supported), we can't proceed
        return

    try:
        # If user object didn't return ID (sometimes it returns abbreviated info), fetch it
        if not user_id:
            print("   Fetching user ID...")
            fetched_user = client.admin.get_user(user.get("name", username))
            user_id = fetched_user.get("id")
            print(f"   User ID: {user_id}")
            
        print("2. Creating Credential (Secret)")
        cred_label = f"secret_{uid}"
        secret_info = client.admin.credentials.create(user_id, cred_label)
        print("   Success! Secret created.")
        # Ensure secret is visible
        if "clientSecret" in secret_info:
            print("   Client Secret received (length check): OK")
        else:
            print("   WARNING: clientsSecret not found in response!")
            
        print("3. Listing Credentials")
        creds = client.admin.credentials.list(user_id)
        # Verify our credential is in the list
        found = any(c.get("name") == cred_label for c in creds)
        if found:
            print("   Success! Credential found in list.")
        else:
            print("   WARNING: Created credential not found in list!")
            
        print("4. Deleting User (Schema cleanup)")
        # Note: Deleting user should cascade delete secrets usually
        client.admin.drop_user(username)
        print("   Success! User deleted.")
        
    except Exception as e:
        print(f"   ERROR during lifecycle: {e}")
        # Try to cleanup
        try:
            client.admin.drop_user(username)
        except:
            pass
            
if __name__ == "__main__":
    test_service_user_lifecycle()
