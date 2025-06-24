"""
Script : feature_dens_com.py
Objective : Compute the density of retail shops (commerce) within each grid cell based on SIRENE data.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for generating spatial features related to shop density.
"""

import pandas as pd 
import geopandas as gpd 
import os  
from appli.scripts.features.features_utils import load_config, print_status  
from shapely.geometry import Point  

def compute_densite_commerces():
    config = load_config()  # Load project configuration
    departement = config["departement"]
    maillage = config["maillage"]

    # Define input and output file paths
    grid_path = f"appli/output/grid/grid_{departement}_{maillage}m.geojson"
    surf_path = f"appli/data/features/surf_batie_{maillage}m.csv"
    sirene_path = "appli/data/sirene/sirene.geojson"
    out_path = f"appli/output/features/densite_commerces_{maillage}m.csv"

    print_status("Loading files", "info")
    # Load spatial grid, built surface area, and SIRENE establishments
    grid = gpd.read_file(grid_path).to_crs("EPSG:2154")
    surf = pd.read_csv(surf_path)
    sirene = gpd.read_file(sirene_path).to_crs("EPSG:2154")

    # Harmonize the type of the join key
    grid["idINSPIRE"] = grid["idINSPIRE"].astype(str)
    surf["idINSPIRE"] = surf["idINSPIRE"].astype(str)

    # Reconstruct the geometry of establishments if missing or empty
    if "geometry" not in sirene.columns or sirene.geometry.is_empty.any():
        sirene["geometry"] = sirene.apply(lambda row: Point(row["longitude"], row["latitude"]), axis=1)
        sirene = gpd.GeoDataFrame(sirene, geometry="geometry", crs="EPSG:2154")

    # Filter retail shops based on NAF code (47 = retail trade)
    sirene["naf2"] = sirene["activitePrincipaleEtablissement"].astype(str).str[:2]
    commerces = sirene[sirene["naf2"] == "47"]

    print_status("Joining shops → grid cells", "info")
    # Spatial join to count the number of shops per grid cell
    joint = gpd.sjoin(grid, commerces, how="left", predicate="contains")
    count = joint.groupby("idINSPIRE").size().reset_index(name="nb_commerces")
    count["idINSPIRE"] = count["idINSPIRE"].astype(str)

    print_status("Calculating density", "info")
    # Merge the number of shops and built surface area for each grid cell
    merged = grid[["idINSPIRE"]].merge(count, on="idINSPIRE", how="left").merge(surf, on="idINSPIRE", how="left")
    merged = merged.fillna(0)  # Replace missing values with 0

    # Apply a minimum surface threshold to avoid outliers
    merged = merged[merged["surf_batie"] > 10]  # safety threshold

    # Compute shop density (number of shops / built surface area)
    merged["densite_commerces"] = merged["nb_commerces"] / merged["surf_batie"]

    # Export the final result (grid cell ID + shop density)
    merged[["idINSPIRE", "densite_commerces"]].to_csv(out_path, index=False)
    print_status("Export completed", "ok", out_path)

# Entry point
if __name__ == "__main__":
    compute_densite_commerces()
