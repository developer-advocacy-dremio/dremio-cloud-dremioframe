
import os
import time
from dremioframe.client import DremioClient
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv()

# We will test against Software since Cloud API is returning 500 for User Creation
MODE = "v26" 

def verify_oauth_login():
    print(f"--- Starting OAuth Login Verification ({MODE}) ---")
    
    # 1. Setup Admin Client
    print("1. Initializing Admin Client...")
    admin_client = DremioClient(mode=MODE)
    
    # 2. Create Temporary Service User
    ts = int(time.time())
    svc_user_name = f"verify_oauth_{ts}"
    print(f"2. Creating Service User '{svc_user_name}'...")
    try:
        user_info = admin_client.admin.create_user(
            username=svc_user_name,
            identity_type="SERVICE_USER"
        )
        # Software API response might vary
        user_id = user_info.get('id')
        print(f"   Created User ID: {user_id}")
    except Exception as e:
        print(f"   Error creating user: {e}")
        # Try to continue if user exists or investigate
        return

    try:
        # 3. Create OAuth Secret
        print("3. Generating Client Secret...")
        # Software endpoint: /api/v3/user/{id}/oauth/credentials ??
        # Let's hope Admin.credentials.create handles it.
        try:
            secret_info = admin_client.admin.credentials.create(user_id, "verify-secret")
            client_secret = secret_info['clientSecret']
            print("   Secret generated.")
        except Exception as e:
            print(f"   Error creating secret: {e}")
            return

        # 4. Attempt Login with Client ID/Secret
        print("4. Testing Login with new credentials...")
        try:
            # We use the user_id as the client_id for Dremio
            # Software uses base_url/oauth/token usually?
            oauth_client = DremioClient(
                mode=MODE,
                hostname=admin_client.hostname,
                client_id=user_id,
                client_secret=client_secret,
                tls=admin_client.tls
            )
            
            # 5. Verify Access
            print("5. Verifying Access (Listing Catalog)...")
            catalog = oauth_client.catalog.list_catalog()
            print(f"   Success! Found {len(catalog)} items.")
            
        except Exception as e:
            print(f"   LOGIN FAILED: {e}")
            import traceback
            traceback.print_exc()

    finally:
        # 6. Cleanup
        print("6. Cleaning up...")
        try:
            admin_client.admin.drop_user(svc_user_name)
            print("   User deleted.")
        except Exception as e:
            print(f"   Error cleaning up: {e}")

if __name__ == "__main__":
    verify_oauth_login()
