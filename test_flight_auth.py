#!/usr/bin/env python3
"""
Test different authentication methods for Arrow Flight on port 32010
"""

import os
from dotenv import load_dotenv
import pyarrow.flight as flight

load_dotenv()

def main():
    host = os.getenv("DREMIO_SOFTWARE_HOST")
    pat = os.getenv("DREMIO_SOFTWARE_PAT")
    
    hostname = host.replace("https://", "").replace("http://", "")
    if ":" in hostname:
        hostname = hostname.split(":")[0]
    
    port = 32010
    
    print("Testing different authentication methods on port 32010...")
    print(f"Hostname: {hostname}")
    print()
    
    # Test 1: Bearer token with TLS
    print("Test 1: Bearer token with grpc+tls")
    try:
        location = f"grpc+tls://{hostname}:{port}"
        client = flight.FlightClient(location)
        headers = [(b"authorization", f"Bearer {pat}".encode("utf-8"))]
        options = flight.FlightCallOptions(headers=headers)
        sql = "SELECT 1 as test"
        info = client.get_flight_info(flight.FlightDescriptor.for_command(sql), options)
        print("  ✅ SUCCESS with Bearer token + TLS!")
        return 0
    except Exception as e:
        print(f"  ❌ Failed: {str(e)[:200]}")
    
    # Test 2: Bearer token without TLS
    print("\nTest 2: Bearer token with grpc+tcp (no TLS)")
    try:
        location = f"grpc+tcp://{hostname}:{port}"
        client = flight.FlightClient(location)
        headers = [(b"authorization", f"Bearer {pat}".encode("utf-8"))]
        options = flight.FlightCallOptions(headers=headers)
        sql = "SELECT 1 as test"
        info = client.get_flight_info(flight.FlightDescriptor.for_command(sql), options)
        print("  ✅ SUCCESS with Bearer token + TCP!")
        return 0
    except Exception as e:
        print(f"  ❌ Failed: {str(e)[:200]}")
    
    # Test 3: _dremio prefix with TLS
    print("\nTest 3: _dremio prefix with grpc+tls")
    try:
        location = f"grpc+tls://{hostname}:{port}"
        client = flight.FlightClient(location)
        headers = [(b"authorization", f"_dremio{pat}".encode("utf-8"))]
        options = flight.FlightCallOptions(headers=headers)
        sql = "SELECT 1 as test"
        info = client.get_flight_info(flight.FlightDescriptor.for_command(sql), options)
        print("  ✅ SUCCESS with _dremio prefix + TLS!")
        return 0
    except Exception as e:
        print(f"  ❌ Failed: {str(e)[:200]}")
    
    # Test 4: _dremio prefix without TLS
    print("\nTest 4: _dremio prefix with grpc+tcp")
    try:
        location = f"grpc+tcp://{hostname}:{port}"
        client = flight.FlightClient(location)
        headers = [(b"authorization", f"_dremio{pat}".encode("utf-8"))]
        options = flight.FlightCallOptions(headers=headers)
        sql = "SELECT 1 as test"
        info = client.get_flight_info(flight.FlightDescriptor.for_command(sql), options)
        print("  ✅ SUCCESS with _dremio prefix + TCP!")
        return 0
    except Exception as e:
        print(f"  ❌ Failed: {str(e)[:200]}")
    
    print("\n❌ All authentication methods failed")
    print("\nThe PAT might need to be exchanged for a session token first.")
    return 1

if __name__ == "__main__":
    exit(main())
