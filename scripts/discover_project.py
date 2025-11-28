import requests
import os
from dotenv import load_dotenv

load_dotenv()

pat = os.getenv("DREMIO_PAT")
if not pat:
    print("No PAT found in .env")
    exit(1)

headers = {"Authorization": f"Bearer {pat}"}
url = "https://api.dremio.cloud/v0/projects"

try:
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print("Projects found:")
        print(response.json())
    else:
        print(f"Failed to list projects: {response.status_code} - {response.text}")
except Exception as e:
    print(f"Error: {e}")
