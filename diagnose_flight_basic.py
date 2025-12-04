#!/usr/bin/env python3
"""
Diagnostic Script Part 2: Testing Basic Auth with PAT
"""

import os
import pyarrow.flight as flight
from dotenv import load_dotenv
from dremioframe.client import DremioClient

load_dotenv()

def test_basic_auth(hostname, port, username, password, description):
    print(f"\nTesting: {description}")
    print(f"  URL: {hostname}:{port}")
    print(f"  Username: {username}")
    print(f"  Password: {'*' * 5}...{'*' * 5}")
    
    location = f"grpc+tls://{hostname}:{port}"
    client = flight.FlightClient(location)
    
    try:
        # Authenticate with Basic Auth
        options = flight.FlightCallOptions()
        auth_handler = client.authenticate_basic_token(username, password)
        
        # auth_handler returns (call_options, token) or just call_options depending on implementation
        # PyArrow's authenticate_basic_token returns a pair (headers, token) usually?
        # Actually, in Dremio's example, it returns a token that we then use.
        # Let's see what it returns.
        print(f"  Auth result type: {type(auth_handler)}")
        
        # If it returns a tuple, the first element is options
        if isinstance(auth_handler, tuple):
            options = auth_handler[0]
        else:
            options = auth_handler
            
        # Try a query
        sql = "SELECT 1 as test"
        descriptor = flight.FlightDescriptor.for_command(sql)
        info = client.get_flight_info(descriptor, options)
        print("  ✅ SUCCESS!")
        return True
    except Exception as e:
        print(f"  ❌ FAILED: {str(e)[:200]}")
        return False

def main():
    print("=" * 80)
    print("Arrow Flight Diagnostic Tool - Part 2")
    print("=" * 80)
    
    host = os.getenv("DREMIO_SOFTWARE_HOST")
    pat = os.getenv("DREMIO_SOFTWARE_PAT")
    
    hostname = host.replace("https://", "").replace("http://", "")
    if ":" in hostname:
        hostname = hostname.split(":")[0]
    port = 32010
    
    # Try to find username from catalog
    print("Attempting to find username from REST API...")
    try:
        client = DremioClient(mode="v26")
        catalog = client.catalog.list_catalog()
        username = None
        for item in catalog:
            if item['path'][0].startswith("@"):
                username = item['path'][0][1:] # Remove @ prefix
                print(f"  Found potential username: {username}")
                break
        
        if not username:
            print("  Could not find username in catalog.")
            # Try the one seen in logs
            username = "alex.merced@dremio.com"
            print(f"  Using fallback username: {username}")
            
    except Exception as e:
        print(f"  Error accessing REST API: {e}")
        username = "alex.merced@dremio.com"
    
    # Test Basic Auth with Username + PAT
    test_basic_auth(hostname, port, username, pat, "Basic Auth (User + PAT)")
    
    # Test Basic Auth with Username + PAT (with @ prefix in username?)
    # Sometimes the home space has @ but username doesn't, or vice versa
    test_basic_auth(hostname, port, f"@{username}", pat, "Basic Auth (@User + PAT)")

if __name__ == "__main__":
    main()
