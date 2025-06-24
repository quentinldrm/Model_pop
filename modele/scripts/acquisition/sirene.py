"""
Script : sirene.py
Objective : Download, clean, and convert SIRENE (establishments) data into GeoParquet.
            The file is processed in chunks to handle its large size.
Author : LEDERMANN Quentin
Date : June 2025
Usage : This script is called by acquisition_pipeline.py to automate the acquisition of SIRENE establishments.
"""

import os
import pandas as pd
import requests
import geopandas as gpd
import numpy as np
from zipfile import ZipFile
from io import BytesIO
from tqdm import tqdm
from typing import Optional, Literal
from shapely.geometry import Point
from modele.scripts.acquisition.acquisition_utils import load_config, print_status

# === SCRIPT PARAMETERS ===
SIRENE_URL = "https://www.data.gouv.fr/fr/datasets/r/0651fb76-bcf3-4f6a-a38d-bc04fa708576"
OUTPUT_DIR = "modele/data/raw"
CACHE_DIR = "modele/cache"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "sirene.parquet")
CHUNK_SIZE = 100_000


# Saves a GeoDataFrame in GeoParquet format with append option
def save_geoparquet(
    gdf: gpd.GeoDataFrame,
    path: str,
    compression: Optional[Literal['snappy', 'gzip', 'brotli']] = "brotli",
    append: bool = False
):
    # If append = True and file already exists: concatenate the old and new data
    if append and os.path.exists(path):
        existing = gpd.read_parquet(path)
        gdf_concat = pd.concat([existing, gdf], ignore_index=True)
        gdf = gpd.GeoDataFrame(gdf_concat, geometry=existing.geometry.name, crs=existing.crs)

    gdf.to_parquet(path, compression=compression, index=False)


# Main function: downloads and processes SIRENE data
def download_sirene():
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print_status("Downloading SIRENE", "info")
    try:
        r = requests.get(SIRENE_URL, timeout=60)
        r.raise_for_status()
    except requests.RequestException as e:
        print_status("Downloading SIRENE", "err", str(e))
        return

    # Deletes the old output file if it exists
    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    try:
        # Directly read the ZIP in memory
        with ZipFile(BytesIO(r.content)) as z:
            with z.open("StockEtablissement_utf8.csv") as csv_file:

                # Read in chunks to avoid saturating RAM
                chunk_iter = pd.read_csv(csv_file, sep=",", dtype=str, chunksize=CHUNK_SIZE)

                for i, chunk in enumerate(tqdm(chunk_iter, desc="Processing SIRENE chunks")):
                    # Convert Lambert coordinates to floats
                    chunk["longitude"] = pd.to_numeric(chunk["coordonneeLambertAbscisseEtablissement"], errors="coerce")
                    chunk["latitude"] = pd.to_numeric(chunk["coordonneeLambertOrdonneeEtablissement"], errors="coerce")

                    # Filter rows with valid coordinates
                    chunk_geo = chunk.dropna(subset=["longitude", "latitude"])
                    chunk_geo = chunk_geo[np.isfinite(chunk_geo["longitude"]) & np.isfinite(chunk_geo["latitude"])]
                    if chunk_geo.empty:
                        continue

                    # Create point geometry
                    chunk_geo["geometry"] = chunk_geo.apply(
                        lambda row: Point(row["longitude"], row["latitude"]), axis=1
                    )
                    chunk_geo = gpd.GeoDataFrame(chunk_geo, geometry="geometry", crs="EPSG:4326")

                    # Save the chunk in append mode
                    save_geoparquet(chunk_geo, OUTPUT_PATH, append=True)

        print_status("SIRENE downloaded and saved in GeoParquet format", "ok")

    except Exception as e:
        print_status("Processing SIRENE data", "err", str(e))


# Entry point if run directly
if __name__ == "__main__":
    download_sirene()
