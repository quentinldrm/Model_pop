"""
Script : h_pond.py
Objective : Compute the average height weighted by built surface area in each 200m grid cell.
            Uses building heights weighted by their ground surface area.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Feature generation step in the features pipeline.
"""

import os
import numpy as np
import pandas as pd
import geopandas as gpd
from modele.scripts.features.features_utils import print_status

# === SCRIPT PARAMETERS ===
BATI_PATH = "modele/data/processed/BATIMENT.parquet"
GRID_PATH = "modele/output/grid/grid_mobiliscope_200m.parquet"
OUTPUT_PATH = "modele/output/features/hauteur_ponderee_surface_200m.csv"


# Main function: average height weighted by built surface area
def compute_hauteur_ponderee_surface(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        if not os.path.exists(BATI_PATH):
            print_status("BATIMENT.parquet file not found", "err", BATI_PATH)
            return pd.DataFrame(columns=["idINSPIRE", "hauteur_ponderee_surface"])

        # Load building data
        bati = gpd.read_parquet(BATI_PATH).to_crs("EPSG:2154")
        bati = bati[bati.geometry.type.isin(["Polygon", "MultiPolygon"])]
        bati["geometry"] = bati["geometry"].buffer(0)  # Fix invalid geometries
        bati = bati[bati["HAUTEUR"].notna()]  # Exclude missing heights

        # Compute surface areas and convert height to float
        bati["area"] = bati.geometry.area
        bati["hauteur"] = pd.to_numeric(bati["HAUTEUR"], errors="coerce")

        # Clean unnecessary columns
        for df in [bati, grid]:
            if "index_right" in df.columns:
                df.drop(columns=["index_right"], inplace=True)

        # Remove invalid cases
        bati = bati.dropna(subset=["hauteur", "area"])
        bati = bati[bati["area"] > 0]

        # Spatial join: buildings to grid cells
        print_status("Spatial join buildings → grid", "info")
        joined = gpd.sjoin(bati, grid, how="inner", predicate="intersects")

        # Weighted calculation: sum(height * surface) / sum(surface)
        joined["prod"] = joined["hauteur"] * joined["area"]
        grouped = joined.groupby("idINSPIRE").agg(
            somme_ponderee=("prod", "sum"),
            somme_surface=("area", "sum")
        ).reset_index()
        grouped["hauteur_ponderee_surface"] = grouped["somme_ponderee"] / grouped["somme_surface"]

        return grouped[["idINSPIRE", "hauteur_ponderee_surface"]]

    except Exception as e:
        print_status("Error computing hauteur_ponderee_surface", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "hauteur_ponderee_surface"])


# Direct execution
if __name__ == "__main__":
    print_status("Computing hauteur_ponderee_surface", "info")

    # Load the grid
    grid = gpd.read_parquet(GRID_PATH).to_crs("EPSG:2154")

    # Compute
    result = compute_hauteur_ponderee_surface(grid)

    # Export
    result.to_csv(OUTPUT_PATH, index=False)
    print_status("Weighted height exported", "ok", OUTPUT_PATH)
