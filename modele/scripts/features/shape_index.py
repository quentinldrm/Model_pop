"""
Script : shape_index.py
Objective : Compute the average shape index of buildings in each 200m grid cell,
            defined as (P² / (4πA)) to evaluate the compactness of built forms.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Feature generation step in the features pipeline.
"""

import geopandas as gpd
import pandas as pd
import numpy as np
import os
from shapely.geometry import Polygon
from modele.scripts.features.features_utils import print_status

# === PATHS ===
BATI_PATH = "modele/data/processed/BATIMENT.parquet"
GRID_PATH = "modele/output/grid/grid_mobiliscope_200m.parquet"
OUTPUT_PATH = "modele/output/features/shape_index_moyen_200m.csv"


# Main function
def compute_shape_index_moyen(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        if not os.path.exists(BATI_PATH):
            print_status("BATIMENT.parquet file not found", "err", BATI_PATH)
            return pd.DataFrame(columns=["idINSPIRE", "shape_index_moyen"])

        # Load and clean geometries
        bati = gpd.read_parquet(BATI_PATH).to_crs("EPSG:2154")
        bati = bati[bati.geometry.type.isin(["Polygon", "MultiPolygon"])]
        bati["geometry"] = bati["geometry"].buffer(0)

        # Compute geometric metrics
        bati["area"] = bati.geometry.area
        bati = bati[bati["area"] > 0]
        bati["perimeter"] = bati.geometry.length
        bati["shape_index"] = (bati["perimeter"] ** 2) / (4 * np.pi * bati["area"])

        # Clean unnecessary columns
        bati.drop(columns=["index_right"], inplace=True, errors="ignore")
        grid.drop(columns=["index_right"], inplace=True, errors="ignore")

        # Spatial join
        joined = gpd.sjoin(bati, grid, how="inner", predicate="intersects")

        # Average shape index per grid cell
        shape_df = joined.groupby("idINSPIRE")["shape_index"].mean().reset_index()
        shape_df.rename(columns={"shape_index": "shape_index_moyen"}, inplace=True)

        return shape_df

    except Exception as e:
        print_status("Error computing shape_index_moyen", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "shape_index_moyen"])


# Direct execution
if __name__ == "__main__":
    print_status("Computing shape_index_moyen", "info")

    grid = gpd.read_parquet(GRID_PATH).to_crs("EPSG:2154")
    result = compute_shape_index_moyen(grid)

    result.to_csv(OUTPUT_PATH, index=False)
    print_status("Average shape index exported", "ok", OUTPUT_PATH)
