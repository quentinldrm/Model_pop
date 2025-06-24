"""
Script : preprocess_area_bati.py
Objective : Compute the built surface area per grid cell by intersecting buildings with the spatial grid.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Preprocessing step for calculating built surface area in the modeling pipeline.
"""

import geopandas as gpd  
import pandas as pd      
import os               
from appli.scripts.features.features_utils import load_config, print_status  

def compute_surface_batie():
    config = load_config()  # Load project configuration
    departement = config["departement"]
    maillage = config["maillage"]

    # Define input and output file paths
    path_grid = f"appli/output/grid/grid_{departement}_{maillage}m.geojson"
    path_bati = "appli/data/topo/BATIMENT.shp"
    path_out = f"appli/data/features/surf_batie_{maillage}m.csv"

    print_status("Loading data", "info")
    # Load spatial grid and buildings, reprojected to EPSG:2154
    grid = gpd.read_file(path_grid).to_crs("EPSG:2154")
    bati = gpd.read_file(path_bati).to_crs("EPSG:2154")

    # Optional: filter empty entities or those with no surface area
    bati = bati[bati.geometry.area > 1]

    print_status("Calculating individual building surfaces", "info")
    # Compute the surface area of each building
    bati["surf"] = bati.geometry.area

    print_status("Spatial intersection buildings → grid", "info")
    # Compute spatial intersection between the grid and buildings
    intersected = gpd.overlay(grid, bati, how="intersection")

    # Calculate built surface area per grid cell (sum of intersected portions)
    print_status("Aggregating by grid cell", "info")
    intersected["surf_partielle"] = intersected.geometry.area
    # Aggregate built surface area by grid cell (idINSPIRE)
    surface = intersected.groupby("idINSPIRE")["surf_partielle"].sum().reset_index()
    surface.rename(columns={"surf_partielle": "surf_batie"}, inplace=True)

    # Create the output directory if needed
    os.makedirs("appli/data/features", exist_ok=True)
    # Export the result to CSV
    surface.to_csv(path_out, index=False)
    print_status("Built surface area export completed", "ok", path_out)

if __name__ == "__main__":
    compute_surface_batie()
