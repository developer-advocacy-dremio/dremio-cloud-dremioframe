import requests
import yaml
import os
import sys

def verify_rest_login():
    # Load profile
    home = os.path.expanduser("~")
    profile_path = os.path.join(home, ".dremio", "profiles.yaml")
    
    with open(profile_path, 'r') as f:
        config = yaml.safe_load(f)
        
    v25 = config['profiles']['v25']
    base_url = v25['base_url'].rstrip('/')
    username = v25['auth']['username']
    password = v25['auth']['password']
    
    print(f"Base URL: {base_url}")
    
    # Try both common paths
    paths = ["/apiv3/login", "/api/v3/login"]
    
    payload = {"userName": username, "password": password}
    
    for path in paths:
        login_url = f"{base_url}{path}"
        print(f"\nTrying: {login_url}")
        try:
            # Allow redirects=False to see if we are being redirected
            response = requests.post(login_url, json=payload, timeout=5, allow_redirects=False)
            print(f"Status Code: {response.status_code}")
            
            if response.status_code == 302:
                print(f"Redirected to: {response.headers.get('Location')}")
            
            if response.status_code == 200:
                ct = response.headers.get('Content-Type', '')
                print(f"Content-Type: {ct}")
                if 'application/json' in ct:
                    token = response.json().get('token')
                    print(f"SUCCESS: Token retrieved: {token[:10]}...")
                    return
                else:
                    print("Received 200 OK but Content-Type is not JSON (likely UI entry point).")
            else:
                 print(f"Failed. Text: {response.text[:100]}...")
                 
        except Exception as e:
            print(f"ERROR: {e}")

if __name__ == "__main__":
    verify_rest_login()
