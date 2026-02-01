
from dremioframe.client import DremioClient
import sys

def test_profile(profile_name):
    print(f"\n--- Testing Profile: {profile_name} ---")
    try:
        client = DremioClient(profile=profile_name)
        print(f"Client initialized. Mode: {client.mode}. Host: {client.hostname}")
        
        # Check Auth Header format
        auth_header = client.session.headers.get('Authorization', '')
        mask_len = 10
        print(f"Auth Header: {auth_header[:20]}... (Total len: {len(auth_header)})")
        
        try:
            # Simple select 1 check
            df = client.query("SELECT 1 as test")
            print(f"Query successful for {profile_name}")
            # print(df)
        except Exception as e:
            print(f"Query Failed for {profile_name}: {e}")
            # traceback.print_exc()

    except Exception as e:
        print(f"Initialization Failed for {profile_name}: {e}")
        if hasattr(e, 'response') and e.response is not None:
             print(f"Response Body: {e.response.text}")

if __name__ == "__main__":
    profiles_to_test = ["cloud", "software", "service", "v25"]
    
    # Write to log file to ensure we capture output
    with open("verify_user_profiles.log", "w") as f:
        sys.stdout = f
        sys.stderr = f
        print(f"Testing library at: {DremioClient.__module__}")
        
        for p in profiles_to_test:
            test_profile(p)
