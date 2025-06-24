"""
Script : secteurs.py
Objective : Merge all GeoJSON files of Mobiliscope sectors into a single GeoParquet.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Initial preprocessing step before grid or feature generation.
"""

import os
import geopandas as gpd
import pandas as pd
from pathlib import Path

# === SCRIPT PARAMETERS ===
INPUT_FOLDER = Path("modele/data/mobiliscope")
OUTPUT_FOLDER = Path("modele/data/processed")
OUTPUT_FILE = OUTPUT_FOLDER / "secteurs.parquet"

# Create the output folder if it doesn't exist
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)


# Main function: merge all found GeoJSON files
def merge_geojson_files():
    """
    Merge all GeoJSON files present in the input folder,
    and export a single GeoParquet file.
    """
    geojson_files = list(INPUT_FOLDER.glob("*.geojson"))
    if not geojson_files:
        print("[✗] No GeoJSON files found in the folder.")
        return

    gdfs = []
    for path in geojson_files:
        try:
            gdf = gpd.read_file(path)
            gdfs.append(gdf)
        except Exception as e:
            print(f"[!] Error reading {path.name}: {e}")

    if not gdfs:
        print("[✗] No valid files to merge.")
        return

    merged = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
    merged.to_parquet(OUTPUT_FILE, compression="brotli", index=False)

    print(f"[✓] Merge completed: {len(merged)} sectors saved to {OUTPUT_FILE}")


# Entry point
if __name__ == "__main__":
    merge_geojson_files()
