"""
Script : recens.py
Objective : Download and process INSEE Recens data (200m grid and Filosofi data),
            filter by department, and export as GeoJSON.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for acquiring and preprocessing Recens data for a specific department.
"""

import os
import requests
import py7zr
import zipfile
import geopandas as gpd
import pandas as pd
import warnings
from appli.scripts.acquisition.download_utils import print_status, load_config

# Suppress pyogrio warnings
warnings.filterwarnings("ignore", category=RuntimeWarning, module="pyogrio")

# URLs for the files
URL_SHAPE = "https://www.insee.fr/fr/statistiques/fichier/6214726/grille200m_shp.7z"
URL_CSV = "https://www.insee.fr/fr/statistiques/fichier/7655475/Filosofi2019_carreaux_200m_csv.7z"
CACHE_DIR = "appli/cache"

# Function to download and extract files from a given URL
def download_and_extract(url, dest_dir):
    filename = url.split("/")[-1]
    local_path = os.path.join(dest_dir, filename)

    # Download the file if it doesn't already exist
    if not os.path.exists(local_path):
        r = requests.get(url, stream=True)
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print_status(f"Download completed: {filename}", "ok")
    else:
        print_status(f"{filename} already exists", "info")

    # Extract the file based on its format
    try:
        if filename.endswith(".7z"):
            with py7zr.SevenZipFile(local_path, mode='r') as archive:
                archive.extractall(path=dest_dir)
        elif filename.endswith(".zip"):
            with zipfile.ZipFile(local_path, 'r') as archive:
                archive.extractall(dest_dir)
        else:
            print_status(f"Unsupported format: {filename}", "err")
            return
        print_status(f"Extraction successful: {filename}", "ok")
    except Exception as e:
        print_status("Error during extraction", "err")

# Function to extract all archive files in the cache directory
def extract_all_archives_in_cache(cache_dir):
    for fname in os.listdir(cache_dir):
        fpath = os.path.join(cache_dir, fname)
        if fname.endswith(".7z"):
            try:
                with py7zr.SevenZipFile(fpath, mode='r') as archive:
                    archive.extractall(path=cache_dir)
                print_status(f"Extraction successful: {fname}", "ok")
            except Exception as e:
                print_status(f"Error extracting {fname}: {e}", "err")
        elif fname.endswith(".zip"):
            try:
                with zipfile.ZipFile(fpath, 'r') as archive:
                    archive.extractall(cache_dir)
                print_status(f"Extraction successful: {fname}", "ok")
            except Exception as e:
                print_status(f"Error extracting {fname}: {e}", "err")

# Main function to download and process Recens data
def download_recens():
    config = load_config("appli/config/settings.yaml")
    dep_code = config.get("departement")
    if not dep_code:
        print_status("Missing department code in settings.yaml", "err")
        return

    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs("appli/data/recens", exist_ok=True)

    # Download and extract files
    download_and_extract(URL_SHAPE, CACHE_DIR)
    download_and_extract(URL_CSV, CACHE_DIR)

    # Extract all archives in the cache directory
    extract_all_archives_in_cache(CACHE_DIR)

    # Load data
    print_status("Loading data", "info")
    gdf = gpd.read_file(os.path.join(CACHE_DIR, "grille200m_metropole.shp"))
    df = pd.read_csv(os.path.join(CACHE_DIR, "carreaux_200m_met.csv"), sep=',', dtype=str)

    # Check for required columns
    if "lcog_geo" not in df.columns:
        print_status("Missing 'lcog_geo' column in the CSV", "err")
        return

    # Perform attribute join
    print_status("Performing attribute join", "info")
    gdf = gdf.rename(columns={"idINSPIRE": "id_car200m"})
    df = df.rename(columns={"idcar_200m": "id_car200m"})
    gdf_joined = gdf.merge(df, on="id_car200m", how="inner")

    # Filter by department using lcog_geo
    print_status(f"Filtering grid cells for department {dep_code}", "info")
    gdf_filtered = gdf_joined[gdf_joined["lcog_geo"].str.startswith(dep_code)]

    # Save the filtered data
    output_path = os.path.join("appli/data/recens", f"recens_{dep_code}.geojson")
    gdf_filtered.to_file(output_path, driver="GeoJSON")
    print_status(f"File saved: {output_path}", "ok")

# Entry point
if __name__ == "__main__":
    download_recens()
