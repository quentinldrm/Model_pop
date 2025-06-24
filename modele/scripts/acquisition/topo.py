"""
Script : topo.py
Objective : Download, extract, and merge BD TOPO BATIMENT shapefiles for all departments.
            The URLs for each department are stored in an external YAML file.
Author : LEDERMANN Quentin
Date : June 2025
Usage : This script is called by acquisition_pipeline.py to automate the acquisition of BD TOPO buildings.
"""

import os
import requests
import yaml
import py7zr
import shutil
import geopandas as gpd
import pandas as pd
from io import BytesIO
from tqdm import tqdm
from typing import Optional, Literal
from modele.scripts.acquisition.acquisition_utils import print_status

# === SCRIPT PARAMETERS ===
YAML_PATH = "utils/topo_url.yaml"
CACHE_DIR = "modele/cache"
BATIMENT_DIR = os.path.join(CACHE_DIR, "batiments")
OUTPUT_DIR = "modele/data/processed"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "BATIMENT.parquet")


# Saves a GeoDataFrame in GeoParquet format with compression
def save_geoparquet(
    gdf: gpd.GeoDataFrame,
    path: str,
    compression: Optional[Literal['snappy', 'gzip', 'brotli']] = "brotli"
):
    gdf.to_parquet(path, compression=compression, index=False)


# Loads BD TOPO URLs from a YAML file (key = department number + suffix)
def get_all_topo_urls():
    with open(YAML_PATH, "r") as f:
        topo_config = yaml.safe_load(f)
    return [(key.split("_")[0], url) for key, url in topo_config["topo_url"].items()]


# Downloads and extracts the .7z file for a given department, and copies the BATIMENT shapefile
def download_and_extract_batiment(num_dept, url, extract_root, batiment_dir):
    try:
        print_status(f"Downloading {num_dept}", "info")
        r = requests.get(url)
        if r.status_code != 200:
            print_status(f"Download error {url}", "err", f"Code {r.status_code}")
            return None

        # Extract the .7z from memory
        with py7zr.SevenZipFile(BytesIO(r.content)) as archive:
            archive.extractall(path=extract_root)

        # Search for the BATIMENT shapefile in the directory structure
        for root, _, files in os.walk(extract_root):
            for file in files:
                name, ext = os.path.splitext(file)
                if name.upper() == "BATIMENT" and ext.lower() == ".shp":
                    for ext2 in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
                        src = os.path.join(root, f"BATIMENT{ext2}")
                        if os.path.exists(src):
                            dst = os.path.join(batiment_dir, f"{num_dept}_BATIMENT{ext2}")
                            shutil.copy2(src, dst)
                    return os.path.join(batiment_dir, f"{num_dept}_BATIMENT.shp")

        print_status(f"No BATIMENT found in {url}", "err")

    except Exception as e:
        print_status(f"Extraction error {url}", "err", str(e))

    finally:
        # Cleanup extracted files (except BATIMENT folder)
        for item in os.listdir(extract_root):
            path = os.path.join(extract_root, item)
            try:
                if not os.path.samefile(path, batiment_dir):
                    if os.path.isdir(path):
                        shutil.rmtree(path)
                    else:
                        os.remove(path)
            except FileNotFoundError:
                continue

    return None


# Merges a list of shapefiles into a single GeoDataFrame, then exports it to GeoParquet
def merge_shapefiles(shapefiles, output_path):
    gdfs = []
    for shp in shapefiles:
        try:
            gdf = gpd.read_file(shp)
            gdfs.append(gdf)
        except Exception as e:
            print_status(f"Error reading {shp}", "err", str(e))

    if gdfs:
        merged = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
        save_geoparquet(merged, output_path)
        print_status(f"Merge completed: {output_path}", "ok")
    else:
        print_status("No shapefiles to merge", "err")


# Main function: downloads, extracts, and merges all departmental BATIMENT files
def download_topo():
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(BATIMENT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    shapefiles = []
    urls = get_all_topo_urls()

    # Download and extract for each department
    for num_dept, url in tqdm(urls, desc="Downloading and extracting"):
        shp = download_and_extract_batiment(num_dept, url, CACHE_DIR, BATIMENT_DIR)
        if shp:
            shapefiles.append(shp)

    # Final merge
    if shapefiles:
        merge_shapefiles(shapefiles, OUTPUT_PATH)


# Entry point
if __name__ == "__main__":
    download_topo()
