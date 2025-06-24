"""
Script : sirene.py
Objective : Download the SIRENE dataset from INSEE, filter by department, and convert it to GeoJSON format.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for acquiring and preprocessing SIRENE data for a specific department.
"""

import os
import pandas as pd
import requests
import geopandas as gpd
import numpy as np
from zipfile import ZipFile
from io import BytesIO
from appli.scripts.acquisition.download_utils import load_config, print_status

# Function to download the SIRENE dataset from INSEE, filter by department, and convert to GeoJSON
def download_sirene():
    # Load configuration to get the department number
    config = load_config()
    departement = config["departement"]
    url = "https://www.data.gouv.fr/fr/datasets/r/0651fb76-bcf3-4f6a-a38d-bc04fa708576"
    output_geojson = f"appli/data/sirene/sirene.geojson"
    os.makedirs("appli/data/sirene", exist_ok=True)

    # Download the SIRENE ZIP file
    print_status("Downloading SIRENE", "info")
    r = requests.get(url)
    if r.status_code != 200:
        print_status("Downloading SIRENE", "err", f"HTTP Code {r.status_code}")
        return

    # Extract the ZIP file and filter by department
    with ZipFile(BytesIO(r.content)) as z:
        with z.open("StockEtablissement_utf8.csv") as csv_file:
            chunks = pd.read_csv(csv_file, sep=",", dtype=str, chunksize=100000)
            df = pd.concat(chunk[chunk["codePostalEtablissement"].str.startswith(departement, na=False)] for chunk in chunks)

    # Process and convert to GeoJSON
    try:
        df["longitude"] = pd.to_numeric(df["coordonneeLambertAbscisseEtablissement"], errors="coerce")
        df["latitude"] = pd.to_numeric(df["coordonneeLambertOrdonneeEtablissement"], errors="coerce")
        df_geo = df.dropna(subset=["longitude", "latitude"])  # Remove rows with invalid coordinates
        df_geo = df_geo[np.isfinite(df_geo["longitude"]) & np.isfinite(df_geo["latitude"])]
        gdf = gpd.GeoDataFrame(
            df_geo,
            geometry=gpd.points_from_xy(df_geo["longitude"], df_geo["latitude"]),
            crs="EPSG:4326"
        ).to_crs("EPSG:2154")
        gdf.to_file(output_geojson, driver="GeoJSON")  # Save as GeoJSON
        print_status("SIRENE downloaded and converted", "ok")
    except Exception as e:
        print_status("SIRENE GeoJSON conversion failed", "err", str(e))

# Entry point
if __name__ == "__main__":
    download_sirene()
