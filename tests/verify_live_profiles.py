
from dremioframe.client import DremioClient
import sys

def test_profile(profile_name):
    print(f"\n--- Testing Profile: {profile_name} ---")
    try:
        client = DremioClient(profile=profile_name)
        print("Client initialized.")
        try:
            # Simple select 1 check
            print(f"Auth Header Prefix: {client.session.headers.get('Authorization', '')[:20]}...")
            df = client.query("SELECT 1 as test")
            print("Query successful:")
            print(df)
        except Exception as e:
            print(f"Query Failed: {e}")
            # print details
            import traceback
            traceback.print_exc()

    except Exception as e:
        print(f"Initialization Failed: {e}")


if __name__ == "__main__":
    with open("verify_results.log", "w") as f:
        sys.stdout = f
        sys.stderr = f
        print(f"Testing library at: {DremioClient.__module__}")
        test_profile("cloud")
        test_profile("software")

