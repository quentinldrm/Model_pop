"""
Script : bpe.py
Objective : Download and convert the Permanent Establishments Database (BPE) into GeoParquet.
Author : LEDERMANN Quentin
Date : June 2025
Usage : This script is called by acquisition_pipeline.py to automate the acquisition of BPE.
"""

import os
import pandas as pd
import requests
import geopandas as gpd
from zipfile import ZipFile
from io import BytesIO
from typing import Literal, Optional
from modele.scripts.acquisition.acquisition_utils import load_config, print_status

# === SCRIPT PARAMETERS ===
BPE_URL = "https://www.insee.fr/fr/statistiques/fichier/8217525/BPE23.zip"
OUTPUT_DIR = "modele/data/processed"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "bpe.parquet")
BPE_CSV_FILENAME = "BPE23.csv"  # Name of the expected file in the ZIP archive


# Saves a GeoDataFrame in GeoParquet format with compression
def save_geoparquet(
    gdf: gpd.GeoDataFrame,
    path: str,
    compression: Optional[Literal['snappy', 'gzip', 'brotli']] = "brotli"
):
    # Saves spatial data compressed for optimized storage
    gdf.to_parquet(path, compression=compression, index=False)


# Main function for downloading and processing BPE data
def download_bpe(force: bool = False):
    """
    Downloads, processes, and converts BPE 2023 data into GeoParquet.
    :param force: Forces download even if the final file already exists.
    """
    # Creates the output folder if it does not exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Avoids reprocessing if the file is already ready
    if os.path.exists(OUTPUT_PATH) and not force:
        print_status("BPE already present - download skipped", "info")
        return

    # Start downloading
    print_status("Downloading BPE", "info")
    try:
        response = requests.get(BPE_URL, timeout=30)  # Timeout added to prevent blocking
        response.raise_for_status()  # Raises an error if the HTTP code is not 200
    except requests.RequestException as e:
        print_status("Downloading BPE", "err", str(e))
        return

    try:
        # Read the ZIP archive
        with ZipFile(BytesIO(response.content)) as z:
            # Checks that the expected CSV file is present
            if BPE_CSV_FILENAME not in z.namelist():
                raise ValueError(f"File {BPE_CSV_FILENAME} not found in the BPE archive")

            # Reads the CSV file into memory
            with z.open(BPE_CSV_FILENAME) as csv_file:
                df = pd.read_csv(csv_file, sep=";", dtype=str, encoding="utf-8")

        # Converts Lambert coordinates
        df["x"] = pd.to_numeric(df["LAMBERT_X"], errors="coerce")
        df["y"] = pd.to_numeric(df["LAMBERT_Y"], errors="coerce")

        # Removes rows without valid coordinates
        df_geo = df.dropna(subset=["x", "y"])
        print_status(f"{len(df_geo)} geolocated rows", "info")

        # Creates a GeoDataFrame with geometric points
        gdf = gpd.GeoDataFrame(
            df_geo,
            geometry=gpd.points_from_xy(df_geo["x"], df_geo["y"]),
            crs="EPSG:2154"  # Projection system used by IGN
        )

        # Saves the GeoDataFrame in compressed format
        save_geoparquet(gdf, OUTPUT_PATH)
        print_status("BPE downloaded and converted", "ok")
    except Exception as e:
        print_status("Processing BPE", "err", str(e))


# Entry point if the script is executed directly
if __name__ == "__main__":
    download_bpe()
