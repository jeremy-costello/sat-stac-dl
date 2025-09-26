import os
import requests
from zipfile import ZipFile

# Define URLs and target folders
downloads = [
    {
        "url": "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lpr_000a21a_e.zip",
        "folder": "./data/inputs/prov_terr"
    },
    {
        "url": "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lcd_000a21a_e.zip",
        "folder": "./data/inputs/census_div"
    },
    {
        "url": "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lcsd000a21a_e.zip",
        "folder": "./data/inputs/census_subdiv"
    },
]

# Ensure folders exist
for d in downloads:
    os.makedirs(d["folder"], exist_ok=True)

# Function to download and unzip
def download_and_unzip(url, target_folder):
    print(f"Downloading {url}...")
    local_zip_path = os.path.join(target_folder, os.path.basename(url))
    
    # Download the file
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_zip_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    
    print(f"Downloaded to {local_zip_path}")
    
    # Unzip
    print(f"Unzipping to {target_folder}...")
    with ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(target_folder)
    
    # Optional: remove the zip file after extraction
    os.remove(local_zip_path)
    print("Done.\n")

# Run downloads
for d in downloads:
    download_and_unzip(d["url"], d["folder"])
