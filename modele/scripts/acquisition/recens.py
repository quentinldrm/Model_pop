"""
Script : recens.py
Objective : Download and merge INSEE population data in 200m grid format with geographic layers,
            for a department or for all metropolitan France.
Author : LEDERMANN Quentin
Date : June 2025
Usage : This script is called by acquisition_pipeline.py to automate the acquisition of census data.
"""

import os
import requests
import py7zr
import zipfile
import geopandas as gpd
import pandas as pd
import warnings
from typing import Optional, Literal
from modele.scripts.acquisition.acquisition_utils import print_status, load_config

# === SCRIPT PARAMETERS ===
URL_SHAPE = "https://www.insee.fr/fr/statistiques/fichier/6214726/grille200m_shp.7z"
URL_CSV = "https://www.insee.fr/fr/statistiques/fichier/7655475/Filosofi2019_carreaux_200m_csv.7z"
CACHE_DIR = "modele/cache"
OUTPUT_DIR = "modele/data/raw"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "recens.parquet")

# Disable annoying warnings related to pyogrio
warnings.filterwarnings("ignore", category=RuntimeWarning, module="pyogrio")


# Saves a GeoDataFrame in GeoParquet format with compression
def save_geoparquet(
    gdf: gpd.GeoDataFrame,
    path: str,
    compression: Optional[Literal['snappy', 'gzip', 'brotli']] = "brotli"
):
    gdf.to_parquet(path, compression=compression, index=False)


# Downloads and extracts a .7z or .zip archive
def download_and_extract(url, dest_dir):
    filename = url.split("/")[-1]
    local_path = os.path.join(dest_dir, filename)

    # Download if the file is not already present
    if not os.path.exists(local_path):
        print_status(f"Downloading {filename}", "info")
        try:
            r = requests.get(url, stream=True)
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print_status(f"Download completed: {filename}", "ok")
        except Exception as e:
            print_status(f"Error during download of {filename}: {e}", "err")
            return
    else:
        print_status(f"{filename} already present", "info")

    # Extraction based on file extension
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
        print_status(f"Error during extraction of {filename}: {e}", "err")


# Downloads and joins census data with geographic grids
def download_recens(metropole=False):
    config = load_config("config/settings.yaml")
    dep_code = config.get("departement")

    # Check that a department code is defined if not processing all of France
    if not dep_code and not metropole:
        print_status("Missing department code in settings.yaml", "err")
        return

    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Download the two required files
    download_and_extract(URL_SHAPE, CACHE_DIR)
    download_and_extract(URL_CSV, CACHE_DIR)

    try:
        # Load the shapefile of 200m grids
        print_status("Loading data", "info")
        gdf_path = os.path.join(CACHE_DIR, "grille200m_metropole.shp")
        df_path = os.path.join(CACHE_DIR, "carreaux_200m_met.csv")

        if not os.path.exists(gdf_path) or not os.path.exists(df_path):
            print_status("Required files missing after extraction", "err")
            return

        gdf = gpd.read_file(gdf_path)
        df = pd.read_csv(df_path, sep=',', dtype=str)
    except Exception as e:
        print_status(f"Error during file loading: {e}", "err")
        return

    # Check that the join key is present
    if "lcog_geo" not in df.columns:
        print_status("Column 'lcog_geo' missing in CSV", "err")
        return

    # Join the geographic layer with attribute data
    print_status("Attribute join", "info")
    gdf = gdf.rename(columns={"idINSPIRE": "id_car200m"})
    df = df.rename(columns={"idcar_200m": "id_car200m"})

    gdf_joined = gdf.merge(df, on="id_car200m", how="inner")

    # Save the result in GeoParquet format
    print_status("Saving joined layer in GeoParquet format", "info")
    try:
        save_geoparquet(gdf_joined, OUTPUT_FILE)
        print_status(f"File saved: {OUTPUT_FILE}", "ok")
    except Exception as e:
        print_status(f"Error during save: {e}", "err")


# Entry point with CLI argument handling
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--metropole", action="store_true", help="Export all metropolitan France")
    args = parser.parse_args()
    download_recens(metropole=args.metropole)
