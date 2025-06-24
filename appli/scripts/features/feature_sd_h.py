"""
Script : feature_sd_h.py
Objective : Compute the standard deviation of building heights within each grid cell.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for generating spatial features related to building height variability.
"""

import geopandas as gpd
import pandas as pd
import os
from appli.scripts.features.features_utils import load_config, print_status

def compute_ecart_type_hauteur(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    # Compute the standard deviation of building heights within each grid cell.
    try:
        config = load_config()  # Load project configuration
        departement = config["departement"]
        path_bati = "appli/data/topo/BATIMENT.shp"

        # Check if the building file exists
        if not os.path.exists(path_bati):
            print_status("BATIMENT.shp file not found", "err", path_bati)
            return pd.DataFrame(columns=["idINSPIRE", "ecart_type_hauteur"])

        # Load buildings and grid
        bati = gpd.read_file(path_bati).to_crs("EPSG:2154")
        bati = bati[bati.geometry.type == "Polygon"]  # Keep only polygons (buildings)
        bati = bati[bati["HAUTEUR"].notna()]  # Filter buildings with valid height data
        bati["hauteur"] = pd.to_numeric(bati["HAUTEUR"], errors="coerce")
        bati = bati.dropna(subset=["hauteur"])  # Remove rows with invalid height values

        # Spatial join with the grid
        joined = gpd.sjoin(bati, grid, how="inner", predicate="intersects")

        # Compute standard deviation of building heights
        result = joined.groupby("idINSPIRE")["hauteur"].std().reset_index()
        result.rename(columns={"hauteur": "ecart_type_hauteur"}, inplace=True)

        return result

    except Exception as e:
        # Handle errors and return an empty DataFrame
        print_status("Error computing standard deviation of building heights", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "ecart_type_hauteur"])

if __name__ == "__main__":
    config = load_config()  # Load project configuration
    maillage = config["maillage"]
    departement = config["departement"]
    grid_path = f"appli/output/grid/grid_{departement}_{maillage}m.geojson"
    grid = gpd.read_file(grid_path).to_crs("EPSG:2154")

    print_status("Computing standard deviation of building heights", "info")
    result = compute_ecart_type_hauteur(grid)
    output_path = f"appli/output/features/ecart_type_hauteur_{maillage}m.csv"
    result.to_csv(output_path, index=False)
    print_status("Standard deviation of building heights exported", "ok", output_path)
