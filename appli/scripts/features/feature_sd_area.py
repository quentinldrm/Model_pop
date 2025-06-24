"""
Script : feature_sd_area.py
Objective : Compute the standard deviation of building surface areas within each grid cell.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for generating spatial features related to building surface variability.
"""

import geopandas as gpd
import pandas as pd
import os
from appli.scripts.features.features_utils import load_config, print_status

def compute_ecart_type_surface_batiment(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    # Compute the standard deviation of building surface areas within each grid cell.
    try:
        config = load_config()  # Load project configuration
        departement = config["departement"]
        path_bati = "appli/data/topo/BATIMENT.shp"

        # Check if the building file exists
        if not os.path.exists(path_bati):
            print_status("BATIMENT.shp file not found", "err", path_bati)
            return pd.DataFrame(columns=["idINSPIRE", "ecart_type_surface_batiment"])

        # Load buildings and grid
        bati = gpd.read_file(path_bati).to_crs("EPSG:2154")
        bati = bati[bati.geometry.type == "Polygon"]  # Keep only polygons (buildings)
        bati["area"] = bati.geometry.area  # Compute surface area
        bati = bati[bati["area"] > 0]  # Filter buildings with valid surface area

        # Spatial join: assign buildings to grid cells
        joined = gpd.sjoin(bati, grid, how="inner", predicate="intersects")

        # Compute standard deviation of surface area
        result = joined.groupby("idINSPIRE")["area"].std().reset_index()
        result.rename(columns={"area": "ecart_type_surface_batiment"}, inplace=True)

        return result

    except Exception as e:
        # Handle errors and return an empty DataFrame
        print_status("Error computing standard deviation of building surface areas", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "ecart_type_surface_batiment"])

if __name__ == "__main__":
    config = load_config()  # Load project configuration
    maillage = config["maillage"]
    departement = config["departement"]
    grid_path = f"appli/output/grid/grid_{departement}_{maillage}m.geojson"
    grid = gpd.read_file(grid_path).to_crs("EPSG:2154")

    print_status("Computing standard deviation of building surface areas", "info")
    result = compute_ecart_type_surface_batiment(grid)
    output_path = f"appli/output/features/ecart_type_surface_batiment_{maillage}m.csv"
    result.to_csv(output_path, index=False)
    print_status("Standard deviation of building surface areas exported", "ok", output_path)
