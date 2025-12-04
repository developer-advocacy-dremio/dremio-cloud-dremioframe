#!/usr/bin/env python3
"""
Comprehensive Arrow Flight Diagnostic Script
Tests various authentication methods and TLS configurations.
"""

import os
import pyarrow.flight as flight
from dotenv import load_dotenv

load_dotenv()

def test_flight(hostname, port, pat, description, tls=True, disable_cert_verification=False, auth_header="Bearer"):
    print(f"\nTesting: {description}")
    print(f"  URL: {hostname}:{port}")
    print(f"  TLS: {tls}")
    print(f"  Verify Certs: {not disable_cert_verification}")
    print(f"  Auth Header: {auth_header} <token>")
    
    protocol = "grpc+tls" if tls else "grpc+tcp"
    location = f"{protocol}://{hostname}:{port}"
    
    try:
        # Configure client options
        kwargs = {}
        if tls and disable_cert_verification:
            # Create a client with disabled verification
            # Note: PyArrow Flight doesn't have a simple "disable_verify" flag in constructor
            # We have to use generic options or override the cert verification
            # For this test, we'll try to use the generic options if possible
            # But standard FlightClient doesn't expose this easily in Python API without custom middleware
            # However, we can try passing 'disable_server_verification' in generic options if supported
            kwargs["disable_server_verification"] = True
            
        client = flight.FlightClient(location, **kwargs)
        
        # Construct headers
        token_val = f"{auth_header} {pat}" if auth_header else pat
        headers = [(b"authorization", token_val.encode("utf-8"))]
        options = flight.FlightCallOptions(headers=headers)
        
        # Try a simple query
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
    print("Arrow Flight Diagnostic Tool")
    print("=" * 80)
    
    host = os.getenv("DREMIO_SOFTWARE_HOST")
    pat = os.getenv("DREMIO_SOFTWARE_PAT")
    
    if not host or not pat:
        print("Error: DREMIO_SOFTWARE_HOST and DREMIO_SOFTWARE_PAT must be set")
        return
        
    hostname = host.replace("https://", "").replace("http://", "")
    if ":" in hostname:
        hostname = hostname.split(":")[0]
        
    port = 32010
    
    # Test 1: Standard Bearer Token
    test_flight(hostname, port, pat, "Standard Bearer Token", tls=True, auth_header="Bearer")
    
    # Test 2: _dremio Token (common for session tokens)
    test_flight(hostname, port, pat, "_dremio Prefix", tls=True, auth_header="_dremio")
    
    # Test 3: No Prefix (Raw Token)
    test_flight(hostname, port, pat, "Raw Token (No Prefix)", tls=True, auth_header="")
    
    # Test 4: Basic Auth style (Bearer with base64?) - unlikely for PAT
    
    # Test 5: Disable Cert Verification (if possible via kwargs)
    test_flight(hostname, port, pat, "Bearer + Disable Cert Verify", tls=True, disable_cert_verification=True, auth_header="Bearer")
    
    # Test 6: TCP (No TLS)
    test_flight(hostname, port, pat, "No TLS (TCP)", tls=False, auth_header="Bearer")

if __name__ == "__main__":
    main()
