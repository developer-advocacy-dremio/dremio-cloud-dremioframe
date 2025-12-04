#!/usr/bin/env python3
"""
Diagnostic Script Part 5: End-to-End Verification
"""

import os
import pyarrow.flight as flight
from dotenv import load_dotenv
from dremioframe.client import DremioClient

load_dotenv()

def main():
    print("=" * 80)
    print("Arrow Flight End-to-End Verification")
    print("=" * 80)
    
    # 1. Initialize Client (v26 mode)
    print("Initializing Client...")
    try:
        client = DremioClient(mode="v26")
        print("✅ Client initialized")
    except Exception as e:
        print(f"❌ Client init failed: {e}")
        return

    # 2. Derive Username from Catalog
    print("\nDeriving Username from Catalog...")
    username = client.username
    if not username:
        try:
            catalog = client.catalog.list_catalog()
            for item in catalog:
                if item['path'][0].startswith("@"):
                    username = item['path'][0][1:]
                    print(f"✅ Found username: {username}")
                    break
            
            if not username:
                print("❌ Could not find username in catalog")
                return
        except Exception as e:
            print(f"❌ Catalog list failed: {e}")
            return
    else:
        print(f"✅ Username already set: {username}")

    # 3. Authenticate Flight
    print("\nAuthenticating Flight...")
    hostname = client.flight_endpoint
    port = client.flight_port
    location = f"grpc+tls://{hostname}:{port}"
    
    flight_client = flight.FlightClient(location)
    
    try:
        # Use Username + PAT as password
        key, value = flight_client.authenticate_basic_token(username, client.pat)
        print(f"✅ Auth successful!")
        print(f"   Key: {key}")
        print(f"   Value: {value}")
        
        # 4. Execute Query
        print("\nExecuting Query...")
        headers = [(key, value)]
        options = flight.FlightCallOptions(headers=headers)
        
        sql = "SELECT 1 as test"
        descriptor = flight.FlightDescriptor.for_command(sql)
        info = flight_client.get_flight_info(descriptor, options)
        reader = flight_client.do_get(info.endpoints[0].ticket, options)
        table = reader.read_all()
        
        print("✅ Query successful!")
        print(table.to_pandas())
        
    except Exception as e:
        print(f"❌ Flight failed: {e}")

if __name__ == "__main__":
    main()
