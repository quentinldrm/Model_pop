"""
Script : feature_h_pond.py
Objective : Compute the height weighted by surface area for buildings within each grid cell.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for generating spatial features related to building heights.
"""

import geopandas as gpd
import pandas as pd
import os
import numpy as np
from appli.scripts.features.features_utils import load_config, print_status

def compute_hauteur_ponderee_surface(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        config = load_config()  # Load project configuration
        departement = config["departement"]
        path_bati = "appli/data/topo/BATIMENT.shp"

        # Check if the building file exists
        if not os.path.exists(path_bati):
            print_status("BATIMENT.shp file not found", "err", path_bati)
            return pd.DataFrame(columns=["idINSPIRE", "hauteur_ponderee_surface"])

        # Load buildings and grid
        bati = gpd.read_file(path_bati).to_crs("EPSG:2154")
        bati = bati[bati.geometry.type == "Polygon"]  # Keep only polygons (buildings)
        bati = bati[bati["HAUTEUR"].notna()]  # Filter buildings with valid height data

        # Compute surface area
        bati["area"] = bati.geometry.area
        bati["hauteur"] = pd.to_numeric(bati["HAUTEUR"], errors="coerce")

        # Remove invalid cases
        bati = bati.dropna(subset=["hauteur", "area"])
        bati = bati[bati["area"] > 0]

        # Spatial join with the grid
        joined = gpd.sjoin(bati, grid, how="inner", predicate="intersects")

        # Weighted height = sum(H * A) / sum(A)
        joined["prod"] = joined["hauteur"] * joined["area"]
        grouped = joined.groupby("idINSPIRE").agg(
            somme_ponderee=("prod", "sum"),
            somme_surface=("area", "sum")
        ).reset_index()
        grouped["hauteur_ponderee_surface"] = grouped["somme_ponderee"] / grouped["somme_surface"]

        return grouped[["idINSPIRE", "hauteur_ponderee_surface"]]

    except Exception as e:
        # Handle errors and return an empty DataFrame
        print_status("Error computing height weighted by surface area", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "hauteur_ponderee_surface"])

if __name__ == "__main__":
    config = load_config()  # Load project configuration
    maillage = config["maillage"]
    departement = config["departement"]
    grid_path = f"appli/output/grid/grid_{departement}_{maillage}m.geojson"
    grid = gpd.read_file(grid_path).to_crs("EPSG:2154")

    print_status("Computing height weighted by surface area", "info")
    result = compute_hauteur_ponderee_surface(grid)
    output_path = f"appli/output/features/hauteur_ponderee_surface_{maillage}m.csv"
    result.to_csv(output_path, index=False)
    print_status("Weighted height exported", "ok", output_path)
