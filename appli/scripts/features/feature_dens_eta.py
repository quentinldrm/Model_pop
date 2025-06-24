"""
Script : feature_dens_eta.py
Objective : Compute the density of establishments within each grid cell (number / built surface area).
Source : SIRENE files + built surface area calculated during preprocessing.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for generating spatial features related to establishment density.
"""

import pandas as pd  
import geopandas as gpd  
import os  
from shapely.geometry import Point  
from appli.scripts.features.features_utils import load_config, print_status  

def compute_densite_etablissements():
    config = load_config()  # Load project configuration
    departement = config["departement"]
    maillage = config["maillage"]

    # Define input/output paths
    grid_path = f"appli/output/grid/grid_{departement}_{maillage}m.geojson"
    surf_path = f"appli/data/features/surf_batie_{maillage}m.csv"
    sirene_path = "appli/data/sirene/sirene.geojson"
    output_path = f"appli/output/features/densite_etablissements_{maillage}m.csv"

    print_status("Loading files", "info")
    # Load spatial grid, built surface area, and SIRENE establishments
    grid = gpd.read_file(grid_path).to_crs("EPSG:2154")
    surf = pd.read_csv(surf_path)
    sirene = gpd.read_file(sirene_path).to_crs("EPSG:2154")

    # Reconstruct geometries if missing or empty
    if "geometry" not in sirene.columns or sirene.geometry.is_empty.any():
        # Convert longitude/latitude to float and remove invalid rows
        sirene["x"] = pd.to_numeric(sirene["longitude"], errors="coerce")
        sirene["y"] = pd.to_numeric(sirene["latitude"], errors="coerce")
        sirene = sirene.dropna(subset=["x", "y"])
        # Create geometry from coordinates
        sirene["geometry"] = [Point(xy) for xy in zip(sirene["x"], sirene["y"])]
        sirene = gpd.GeoDataFrame(sirene, geometry="geometry", crs="EPSG:2154")

    # Harmonize idINSPIRE type (convert to string everywhere)
    grid["idINSPIRE"] = grid["idINSPIRE"].astype(str)
    surf["idINSPIRE"] = surf["idINSPIRE"].astype(str)

    # Spatial join: assign each establishment to a grid cell
    print_status("Spatial join SIRENE → grid cells", "info")
    joined = gpd.sjoin(grid, sirene, how="left", predicate="contains")

    # Count the number of establishments per grid cell
    count = joined.groupby("idINSPIRE").size().reset_index(name="nb_etabs_sirene")
    count["idINSPIRE"] = count["idINSPIRE"].astype(str)

    print_status("Merging with built surface area", "info")
    # Merge the number of establishments and built surface area for each grid cell
    df = grid[["idINSPIRE"]].merge(count, on="idINSPIRE", how="left").merge(surf, on="idINSPIRE", how="left")
    df = df.fillna(0)  # Replace missing values with 0

    # Remove grid cells with very low built surface area (to avoid outliers)
    df = df[df["surf_batie"] > 10]

    print_status("Calculating densities", "info")
    # Compute establishment density (number / built surface area)
    df["densite_etablissements"] = df["nb_etabs_sirene"] / df["surf_batie"]

    # Export the final result (grid cell ID + establishment density)
    df[["idINSPIRE", "densite_etablissements"]].to_csv(output_path, index=False)
    print_status("Export completed", "ok", output_path)

# Entry point
if __name__ == "__main__":
    compute_densite_etablissements()  # Run the computation if the script is executed directly
