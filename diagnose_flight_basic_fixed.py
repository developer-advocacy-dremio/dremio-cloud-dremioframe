#!/usr/bin/env python3
"""
Diagnostic Script Part 3: Testing Basic Auth with PAT (Fixed)
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
    
    location = f"grpc+tls://{hostname}:{port}"
    client = flight.FlightClient(location)
    
    try:
        # Authenticate with Basic Auth
        options = flight.FlightCallOptions()
        print("  Authenticating...")
        auth_result = client.authenticate_basic_token(username, password)
        
        print(f"  Auth result type: {type(auth_result)}")
        if isinstance(auth_result, tuple):
            print(f"  Tuple length: {len(auth_result)}")
            print(f"  Element 0 type: {type(auth_result[0])}")
            if len(auth_result) > 1:
                print(f"  Element 1 type: {type(auth_result[1])}")
        
        # Handle return value
        # PyArrow documentation says it returns a pair (call_options, token)
        # But implementation might vary.
        # If we got bytes, it's likely the token.
        
        call_options = None
        
        if isinstance(auth_result, tuple):
            # Try to find the token and options
            token = None
            for item in auth_result:
                if isinstance(item, bytes):
                    token = item
                elif isinstance(item, flight.FlightCallOptions):
                    call_options = item
            
            if token and not call_options:
                # Construct options from token
                print("  Got token (bytes), constructing options...")
                headers = [(b"authorization", b"Bearer " + token)]
                call_options = flight.FlightCallOptions(headers=headers)
            elif not call_options and not token:
                # Maybe (bytes, str)?
                print(f"  Unknown tuple structure: {auth_result}")
                return False
        elif isinstance(auth_result, bytes):
             print("  Got raw bytes token, constructing options...")
             headers = [(b"authorization", b"Bearer " + auth_result)]
             call_options = flight.FlightCallOptions(headers=headers)
        else:
            call_options = auth_result
            
        # Try a query
        print("  Executing query...")
        sql = "SELECT 1 as test"
        descriptor = flight.FlightDescriptor.for_command(sql)
        info = client.get_flight_info(descriptor, call_options)
        print("  ✅ SUCCESS!")
        return True
    except Exception as e:
        print(f"  ❌ FAILED: {str(e)[:200]}")
        return False

def main():
    print("=" * 80)
    print("Arrow Flight Diagnostic Tool - Part 3")
    print("=" * 80)
    
    host = os.getenv("DREMIO_SOFTWARE_HOST")
    pat = os.getenv("DREMIO_SOFTWARE_PAT")
    
    hostname = host.replace("https://", "").replace("http://", "")
    if ":" in hostname:
        hostname = hostname.split(":")[0]
    port = 32010
    
    username = "alex.merced@dremio.com"
    
    # Test Basic Auth with Username + PAT
    test_basic_auth(hostname, port, username, pat, "Basic Auth (User + PAT)")

if __name__ == "__main__":
    main()
