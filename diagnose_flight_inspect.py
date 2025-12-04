#!/usr/bin/env python3
"""
Diagnostic Script Part 4: Inspecting Auth Return Value
"""

import os
import pyarrow.flight as flight
from dotenv import load_dotenv

load_dotenv()

def inspect_auth(hostname, port, username, password):
    print(f"Connecting to {hostname}:{port} as {username}")
    location = f"grpc+tls://{hostname}:{port}"
    client = flight.FlightClient(location)
    
    try:
        print("Authenticating...")
        result = client.authenticate_basic_token(username, password)
        
        print(f"Result type: {type(result)}")
        if isinstance(result, tuple):
            print(f"Tuple length: {len(result)}")
            for i, item in enumerate(result):
                print(f"Item {i} type: {type(item)}")
                if isinstance(item, bytes):
                    try:
                        print(f"Item {i} decoded: {item.decode('utf-8')}")
                    except:
                        print(f"Item {i} raw: {item}")
                else:
                    print(f"Item {i}: {item}")
                    
    except Exception as e:
        print(f"Error: {e}")

def main():
    host = os.getenv("DREMIO_SOFTWARE_HOST")
    pat = os.getenv("DREMIO_SOFTWARE_PAT")
    
    hostname = host.replace("https://", "").replace("http://", "")
    if ":" in hostname:
        hostname = hostname.split(":")[0]
    port = 32010
    username = "alex.merced@dremio.com"
    
    inspect_auth(hostname, port, username, pat)

if __name__ == "__main__":
    main()
