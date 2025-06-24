"""
Script : larg_moy_voirie.py
Objective : Compute the weighted average width of road segments in each 200m grid cell,
            using the width provided in the ROUTE layer and the geometric length.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Feature generation step in the features pipeline.
"""

import os
import pandas as pd
import geopandas as gpd
from modele.scripts.features.features_utils import print_status

# === SCRIPT PARAMETERS ===
ROUTE_PATH = "modele/data/processed/ROUTE.parquet"
GRID_PATH = "modele/output/grid/grid_mobiliscope_200m.parquet"
OUTPUT_PATH = "modele/output/features/largeur_moyenne_voirie_200m.csv"


# Main function
def compute_largeur_moyenne_voirie(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        if not os.path.exists(ROUTE_PATH):
            print_status("ROUTE.parquet file not found", "err", ROUTE_PATH)
            return pd.DataFrame(columns=["idINSPIRE", "largeur_moyenne_voirie"])

        # Load and clean data
        voirie = gpd.read_parquet(ROUTE_PATH).to_crs("EPSG:2154")
        voirie = voirie[voirie["LARGEUR"].notna()]
        voirie["largeur"] = pd.to_numeric(voirie["LARGEUR"], errors="coerce")
        voirie["longueur"] = voirie.geometry.length
        voirie = voirie.dropna(subset=["largeur", "longueur"])

        # Clean inherited columns
        for df in [voirie, grid]:
            df.drop(columns=["index_right"], inplace=True, errors="ignore")

        # Spatial join
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
        print_status("Error computing largeur_moyenne_voirie", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "largeur_moyenne_voirie"])


# Direct execution
if __name__ == "__main__":
    print_status("Computing largeur_moyenne_voirie", "info")

    grid = gpd.read_parquet(GRID_PATH).to_crs("EPSG:2154")
    result = compute_largeur_moyenne_voirie(grid)

    result.to_csv(OUTPUT_PATH, index=False)
    print_status("Average width exported", "ok", OUTPUT_PATH)
