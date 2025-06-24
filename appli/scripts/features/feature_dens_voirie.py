"""
Script : dens_voirie.py
Objective : Compute the road density (in km/km²) within each 200m grid cell,
            based on the linear road layer.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Feature generation step in the feature pipeline.
"""

import geopandas as gpd
import pandas as pd
import os
from appli.scripts.features.features_utils import print_status,load_config

config = load_config()
departement = config["departement"]
maillage = config["maillage"]

# === SCRIPT PARAMETERS ===
PATH_ROUTE = "appli/data/topo/TRONCON_DE_ROUTE.shp"
GRID_PATH = f"appli/output/grid/grid_{departement}_{maillage}m.geojson"
OUTPUT_PATH = f"appli/output/features/densite_voirie_{maillage}m.csv"
MAILLE_SURFACE_KM2 = 0.04  # surface of a 200m x 200m grid cell in km²

# Main function for optimized road density calculation
def compute_densite_voirie_optimisee():

    config = load_config()
    departement = config["departement"]
    maillage = config["maillage"]

    try:
        if not os.path.exists(PATH_ROUTE):
            print_status("TRONCON_DE_ROUTE.shp file not found", "err", PATH_ROUTE)
            return

        if not os.path.exists(GRID_PATH):
            print_status("Grid file not found", "err", GRID_PATH)
            return

        print_status("Loading data...", "info")
        voirie = gpd.read_file(PATH_ROUTE).to_crs("EPSG:2154")
        grid = gpd.read_file(GRID_PATH).to_crs("EPSG:2154")

        # Spatial filtering using bounding box
        print_status("Spatial filtering by bounding box...", "info")
        bbox = grid.total_bounds
        voirie = voirie.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]]

        # Compute road lengths (in km)
        print_status("Computing road lengths...", "info")
        voirie["longueur_km"] = voirie.geometry.length / 1000
        grid["surface_km2"] = grid.geometry.area / 1e6

        # Spatial join: roads → grid cells
        print_status("Spatial join between roads and grid...", "info")
        voirie = voirie.drop(columns=['index_right'], errors='ignore')
        grid = grid.drop(columns=['index_right'], errors='ignore')
        joined = gpd.sjoin(voirie, grid, how="inner", predicate="intersects")
        joined = joined.rename(columns={"geometry": "geometry_voirie", "index_right": "grid_index"})

        # Compute exact intersections between roads and grid cells
        print_status("Computing geometric intersections...", "info")
        joined["geometry"] = joined.apply(
            lambda row: row["geometry_voirie"].intersection(grid.iloc[row["grid_index"]].geometry),
            axis=1
        )
        joined["longueur_intersect_km"] = joined.geometry.length / 1000
        joined["idINSPIRE"] = grid.iloc[joined["grid_index"]]["idINSPIRE"].values

        # Aggregate by grid cell
        print_status("Aggregating by grid cell...", "info")
        result = joined.groupby("idINSPIRE")["longueur_intersect_km"].sum().reset_index()
        result["densite_voirie"] = result["longueur_intersect_km"] / MAILLE_SURFACE_KM2

        # Export results
        result.to_csv(OUTPUT_PATH, index=False)
        print_status("Road density successfully exported", "ok", OUTPUT_PATH)

    except Exception as e:
        print_status(f"Error during optimized calculation: {str(e)}", "err")


# Entry point
if __name__ == "__main__":
    compute_densite_voirie_optimisee()
