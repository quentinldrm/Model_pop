"""
Script : sd_area.py
Objective : Compute the standard deviation of building surface areas per 200m grid cell,
            to measure the morphological variability of the built environment.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Feature generation step in the features pipeline.
"""

import geopandas as gpd
import pandas as pd
import os
from modele.scripts.features.features_utils import print_status

# === PATHS ===
BATI_PATH = "modele/data/processed/BATIMENT.parquet"
GRID_PATH = "modele/output/grid/grid_mobiliscope_200m.parquet"
OUTPUT_PATH = "modele/output/features/ecart_type_surface_batiment_200m.csv"


# Main function
def compute_ecart_type_surface_batiment(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        if not os.path.exists(BATI_PATH):
            print_status("BATIMENT.parquet file not found", "err", BATI_PATH)
            return pd.DataFrame(columns=["idINSPIRE", "ecart_type_surface_batiment"])

        # Load buildings
        bati = gpd.read_parquet(BATI_PATH).to_crs("EPSG:2154")
        bati = bati[bati.geometry.type.isin(["Polygon", "MultiPolygon"])]
        bati["geometry"] = bati["geometry"].buffer(0)  # Fix invalid geometries
        bati["area"] = bati.geometry.area
        bati = bati[bati["area"] > 0]

        # Clean inherited indexes
        for df in [bati, grid]:
            df.drop(columns=["index_right"], inplace=True, errors="ignore")

        # Spatial join
        print_status("Spatial join buildings → grid", "info")
        joined = gpd.sjoin(bati, grid, how="inner", predicate="intersects")

        # Compute standard deviation of surface areas
        print_status("Computing standard deviation of surface areas", "info")
        result = joined.groupby("idINSPIRE")["area"].std().reset_index()
        result.rename(columns={"area": "ecart_type_surface_batiment"}, inplace=True)

        return result

    except Exception as e:
        print_status("Error computing ecart_type_surface_batiment", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "ecart_type_surface_batiment"])


# Direct execution
if __name__ == "__main__":
    print_status("Computing ecart_type_surface_batiment", "info")

    grid = gpd.read_parquet(GRID_PATH).to_crs("EPSG:2154")
    result = compute_ecart_type_surface_batiment(grid)

    result.to_csv(OUTPUT_PATH, index=False)
    print_status("Standard deviation of surface areas exported", "ok", OUTPUT_PATH)
