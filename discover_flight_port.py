#!/usr/bin/env python3
"""
Arrow Flight Port Discovery Script for Dremio Software v26

This script attempts to connect to common Dremio Arrow Flight ports
to help you discover the correct configuration.
"""

import os
from dotenv import load_dotenv
import pyarrow.flight as flight

# Load environment variables
load_dotenv()

def test_flight_port(hostname, port, pat, tls=True):
    """Test if a given port accepts Arrow Flight connections."""
    protocol = "grpc+tls" if tls else "grpc+tcp"
    location = f"{protocol}://{hostname}:{port}"
    
    try:
        client = flight.FlightClient(location)
        
        # Try to authenticate with PAT
        headers = [
            (b"authorization", f"Bearer {pat}".encode("utf-8"))
        ]
        options = flight.FlightCallOptions(headers=headers)
        
        # Try a simple query to test the connection
        sql = "SELECT 1 as test"
        descriptor = flight.FlightDescriptor.for_command(sql)
        
        # This will raise an exception if the connection fails
        info = client.get_flight_info(descriptor, options)
        
        return True, "Success"
    except Exception as e:
        return False, str(e)

def main():
    print("=" * 80)
    print("Dremio Arrow Flight Port Discovery")
    print("=" * 80)
    
    # Get configuration
    host = os.getenv("DREMIO_SOFTWARE_HOST")
    tls = os.getenv("DREMIO_SOFTWARE_TLS", "false").lower() == "true"
    pat = os.getenv("DREMIO_SOFTWARE_PAT")
    
    if not host or not pat:
        print("\n❌ ERROR: DREMIO_SOFTWARE_HOST and DREMIO_SOFTWARE_PAT must be set")
        return 1
    
    # Extract hostname
    hostname = host.replace("https://", "").replace("http://", "")
    if ":" in hostname:
        hostname = hostname.split(":")[0]
    
    print(f"\nTesting Arrow Flight connection to: {hostname}")
    print(f"TLS Enabled: {tls}")
    print()
    
    # Common Dremio Arrow Flight ports to try
    ports_to_try = [
        (32010, "Standard Dremio Software Arrow Flight port"),
        (31010, "Alternative Dremio port"),
        (47470, "Dremio Cloud Arrow Flight port"),
        (443, "HTTPS port (if Flight uses same as REST)"),
        (9047, "Dremio REST API port"),
    ]
    
    print("Testing common Arrow Flight ports...")
    print("-" * 80)
    
    successful_ports = []
    
    for port, description in ports_to_try:
        print(f"\nTesting port {port} ({description})...")
        success, message = test_flight_port(hostname, port, pat, tls)
        
        if success:
            print(f"  ✅ SUCCESS! Port {port} is working")
            successful_ports.append(port)
        else:
            # Truncate long error messages
            short_message = message[:100] + "..." if len(message) > 100 else message
            print(f"  ❌ Failed: {short_message}")
    
    print("\n" + "=" * 80)
    
    if successful_ports:
        print("✅ DISCOVERY SUCCESSFUL!")
        print(f"\nWorking Arrow Flight port(s): {', '.join(map(str, successful_ports))}")
        print(f"\nAdd this to your .env file:")
        print(f"  DREMIO_SOFTWARE_FLIGHT_PORT={successful_ports[0]}")
    else:
        print("❌ NO WORKING PORTS FOUND")
        print("\nPossible reasons:")
        print("  1. Arrow Flight is not enabled on this Dremio instance")
        print("  2. The Flight port is different from the common ports tested")
        print("  3. Firewall or network configuration is blocking the connection")
        print("  4. The PAT doesn't have permissions for Flight queries")
        print("\nNext steps:")
        print("  - Contact your Dremio administrator for the correct Flight port")
        print("  - Check Dremio server configuration for Arrow Flight settings")
        print("  - Verify your PAT has query execution permissions")
    
    print("=" * 80)
    return 0 if successful_ports else 1

if __name__ == "__main__":
    exit(main())
