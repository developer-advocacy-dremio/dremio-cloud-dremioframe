import pytest
import os
from dremioframe.client import DremioClient

@pytest.mark.software
@pytest.mark.skipif(not os.environ.get("DREMIO_SOFTWARE_HOST"), 
                    reason="Dremio Software credentials not found")
def test_software_connection_live():
    client = DremioClient(
        hostname=os.environ.get("DREMIO_SOFTWARE_HOST"),
        port=int(os.environ.get("DREMIO_SOFTWARE_PORT", 32010)),
        username=os.environ.get("DREMIO_SOFTWARE_USER"),
        password=os.environ.get("DREMIO_SOFTWARE_PASSWORD"),
        tls=os.environ.get("DREMIO_SOFTWARE_TLS", "false").lower() == "true"
    )
    
    # Simple catalog list to verify connection
    catalog = client.catalog.list_catalog()
    assert isinstance(catalog, list)
