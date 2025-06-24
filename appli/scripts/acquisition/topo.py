"""
Script : topo.py
Objective : Download the BD TOPO dataset from the specified URL in topo_url.yaml,
            extract the BATIMENT and TRONCON_DE_ROUTE files, and save them to the output directory.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for acquiring and preprocessing BD TOPO data for a specific department.
"""

import os
import requests
import yaml
import py7zr
from io import BytesIO
import shutil
from appli.scripts.acquisition.download_utils import load_config, print_status

# Function to retrieve the BD TOPO URL from the configuration file
def get_topo_url():
    with open("appli/config/topo_url.yaml", "r") as f:
        topo_config = yaml.safe_load(f)
    departement = load_config()["departement"]
    return topo_config["topo_url"].get(f"{departement}_url")

# Function to download and process the BD TOPO dataset
def download_topo():
    url = get_topo_url()
    if not url:
        print_status("BD TOPO", "err", "Missing URL in topo_url.yaml")
        return

    extract_root = "appli/cache"
    output_dir = "appli/data/topo"
    os.makedirs(extract_root, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Download the BD TOPO archive
        print_status("Downloading BD TOPO", "info")
        r = requests.get(url)
        if r.status_code != 200:
            raise Exception(f"HTTP Code {r.status_code}")
        with py7zr.SevenZipFile(BytesIO(r.content)) as archive:
            archive.extractall(path=extract_root)
        print_status("BD TOPO extraction successful", "ok")

        # Search for BATIMENT and TRONCON_DE_ROUTE files in the extracted directory
        found = False
        for root, dirs, files in os.walk(extract_root):
            for file in files:
                name, _ = os.path.splitext(file)
                if name.upper() == "BATIMENT" and name.upper() == "TRONCON_DE_ROUTE":
                    src = os.path.join(root, file)
                    dst = os.path.join(output_dir, file)
                    shutil.copy2(src, dst)
                    found = True

        if found:
            print_status("BATIMENT and TRONCON_DE_ROUTE files extracted", "ok")
        else:
            print_status("No BATIMENT or TRONCON_DE_ROUTE files found", "err")

    except Exception as e:
        print_status("BD TOPO download failed", "err", str(e))
    finally:
        # Clean up the extraction directory
        if os.path.exists(extract_root):
            shutil.rmtree(extract_root)

# Entry point
if __name__ == "__main__":
    download_topo()
