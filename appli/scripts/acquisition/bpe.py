"""
Script : bpe.py
Objective : Download the BPE (Base Permanente des Établissements) dataset from INSEE,
            filter it by department, and convert it to GeoJSON format.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for acquiring and preprocessing BPE data for a specific department.
"""

import os
import pandas as pd
import requests
import geopandas as gpd
from zipfile import ZipFile
from io import BytesIO
from appli.scripts.acquisition.download_utils import load_config, print_status

# Function to download the BPE dataset from INSEE, filter by department, and convert to GeoJSON
def download_bpe():
    # Load configuration to get the department number
    config = load_config()
    departement = config["departement"]
    url = "https://www.insee.fr/fr/statistiques/fichier/8217525/BPE23.zip"
    output_geojson = f"appli/data/bpe/bpe_{departement}.geojson"
    os.makedirs("appli/data/bpe", exist_ok=True)

    # Download the BPE ZIP file
    print_status("Downloading BPE", "info")
    r = requests.get(url)
    if r.status_code != 200:
        print_status("Downloading BPE", "err", f"HTTP Code {r.status_code}")
        return

    # Extract the ZIP file and filter by department
    with ZipFile(BytesIO(r.content)) as z:
        with z.open("BPE23.csv") as csv_file:
            df = pd.read_csv(csv_file, sep=";", dtype=str, encoding="utf-8")
            df_filtered = df[df["DEP"].str.startswith(departement, na=False)].copy()

    # Process and convert to GeoJSON
    try:
        df_filtered["x"] = pd.to_numeric(df_filtered["LAMBERT_X"], errors="coerce")
        df_filtered["y"] = pd.to_numeric(df_filtered["LAMBERT_Y"], errors="coerce")
        df_geo = df_filtered.dropna(subset=["x", "y"])  # Remove rows with invalid coordinates
        gdf = gpd.GeoDataFrame(df_geo, geometry=gpd.points_from_xy(df_geo["x"], df_geo["y"]), crs="EPSG:2154")
        gdf.to_file(output_geojson, driver="GeoJSON")  # Save as GeoJSON
        print_status("BPE downloaded and converted", "ok")
    except Exception as e:
        print_status("BPE GeoJSON conversion failed", "err", str(e))

# Entry point
if __name__ == "__main__":
    download_bpe()