"""
Script : feature_shape_index.py
Objective : Compute the average shape index of buildings within each grid cell.
            The shape index measures the compactness of a polygon based on its perimeter and area.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for generating spatial features related to building compactness.
"""

import geopandas as gpd
import pandas as pd
import numpy as np
import os
from shapely.geometry import Polygon
from appli.scripts.features.features_utils import load_config, print_status

def compute_shape_index_moyen(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    # Compute the average shape index of buildings within each grid cell.
    try:
        config = load_config()  # Load project configuration
        departement = config["departement"]
        maillage = int(config["maillage"])
        path_bati = "appli/data/topo/BATIMENT.shp"

        # Check if the building file exists
        if not os.path.exists(path_bati):
            print_status("BATIMENT.shp file not found", "err", path_bati)
            return pd.DataFrame(columns=["idINSPIRE", "shape_index_moyen"])

        # Load buildings and grid
        bati = gpd.read_file(path_bati).to_crs("EPSG:2154")
        bati = bati[bati.geometry.type == "Polygon"]  # Ensure the geometries are polygons

        # Compute surface area and perimeter
        bati["area"] = bati.geometry.area
        bati["perimeter"] = bati.geometry.length

        # Compute the shape index
        bati = bati[bati["area"] > 0]  # Filter buildings with valid surface area
        bati["shape_index"] = (bati["perimeter"] ** 2) / (4 * np.pi * bati["area"])

        # Spatial join with the grid
        joined = gpd.sjoin(bati, grid, how="inner", predicate="intersects")

        # Compute the average shape index per grid cell
        shape_df = joined.groupby("idINSPIRE")["shape_index"].mean().reset_index()
        shape_df.rename(columns={"shape_index": "shape_index_moyen"}, inplace=True)

        return shape_df

    except Exception as e:
        # Handle errors and return an empty DataFrame
        print_status("Error computing average shape index", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "shape_index_moyen"])

if __name__ == "__main__":
    config = load_config()  # Load project configuration
    maillage = config["maillage"]
    departement = config["departement"]
    grid_path = f"appli/output/grid/grid_{departement}_{maillage}m.geojson"
    grid = gpd.read_file(grid_path).to_crs("EPSG:2154")

    print_status("Computing average shape index", "info")
    result = compute_shape_index_moyen(grid)
    output_path = f"appli/output/features/shape_index_moyen_{maillage}m.csv"
    result.to_csv(output_path, index=False)
    print_status("Average shape index exported", "ok", output_path)
