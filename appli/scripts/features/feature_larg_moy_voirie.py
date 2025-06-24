"""
Script : larg_moy_voirie.py
Objective : Compute the weighted average width of road segments within each 200m grid cell,
            using the width provided in the ROUTE layer and the geometric length.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Feature generation step in the feature pipeline.
"""

import os
import pandas as pd
import geopandas as gpd
from appli.scripts.features.features_utils import print_status, load_config

config = load_config()
departement = config["departement"]
maillage = config["maillage"]

# === SCRIPT PARAMETERS ===
ROUTE_PATH = "appli/data/topo/TRONCON_DE_ROUTE.shp"
GRID_PATH = f"appli/output/grid/grid_{departement}_{maillage}m.geojson"
OUTPUT_PATH = f"appli/output/features/largeur_moyenne_voirie_{maillage}m.csv"


# Main function
def compute_largeur_moyenne_voirie(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        # Check if the road file exists
        if not os.path.exists(ROUTE_PATH):
            print_status("TRONCON_DE_ROUTE.shp file not found", "err", ROUTE_PATH)
            return pd.DataFrame(columns=["idINSPIRE", "largeur_moyenne_voirie"])

        # Load and clean data
        voirie = gpd.read_file(ROUTE_PATH).to_crs("EPSG:2154")
        voirie = voirie[voirie["LARGEUR"].notna()]  # Filter rows with valid width data
        voirie["largeur"] = pd.to_numeric(voirie["LARGEUR"], errors="coerce")
        voirie["longueur"] = voirie.geometry.length  # Compute road segment lengths
        voirie = voirie.dropna(subset=["largeur", "longueur"])  # Remove invalid rows

        # Clean inherited columns
        for df in [voirie, grid]:
            df.drop(columns=["index_right"], inplace=True, errors="ignore")

        # Spatial join: assign road segments to grid cells
        joined = gpd.sjoin(voirie, grid, how="inner", predicate="intersects")

        # Weighted average width: sum(length * width) / sum(length)
        joined["largeur_pondérée"] = joined["largeur"] * joined["longueur"]
        grouped = joined.groupby("idINSPIRE").agg(
            somme_largeur=("largeur_pondérée", "sum"),
            somme_longueur=("longueur", "sum")
        ).reset_index()
        grouped["largeur_moyenne_voirie"] = grouped["somme_largeur"] / grouped["somme_longueur"]

        return grouped[["idINSPIRE", "largeur_moyenne_voirie"]]

    except Exception as e:
        # Handle errors and return an empty DataFrame
        print_status("Error computing weighted average road width", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "largeur_moyenne_voirie"])


# Direct execution
if __name__ == "__main__":
    print_status("Computing weighted average road width", "info")

    grid = gpd.read_file(GRID_PATH).to_crs("EPSG:2154")
    result = compute_largeur_moyenne_voirie(grid)

    result.to_csv(OUTPUT_PATH, index=False)
    print_status("Weighted average road width exported", "ok", OUTPUT_PATH)
