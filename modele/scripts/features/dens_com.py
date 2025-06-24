"""
Script : dens_com.py
Objective : Compute the density of shops (NAF code 47) per 200m grid cell,
            by dividing the number of shops by the built surface area.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Feature generation step in the features pipeline.
"""

import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from modele.scripts.features.features_utils import load_config, print_status

# === SCRIPT PARAMETERS ===
GRID_PATH = "modele/output/grid/grid_mobiliscope_200m.parquet"
SURF_PATH = "modele/data/processed/surf_bati_200m.csv"
SIRENE_PATH = "modele/data/raw/sirene.parquet"
OUTPUT_PATH = "modele/output/features/densite_commerces_200m.csv"


# Main function to compute the density of shops
def compute_densite_commerces():
    print_status("Loading files", "info")

    # Read input files
    grid = gpd.read_parquet(GRID_PATH).to_crs("EPSG:2154")
    surf = pd.read_csv(SURF_PATH)
    sirene = gpd.read_parquet(SIRENE_PATH).to_crs("EPSG:2154")

    # Clean inherited indexes
    for df in [grid, surf]:
        if "index_right" in df.columns:
            df.drop(columns=["index_right"], inplace=True)
    grid["idINSPIRE"] = grid["idINSPIRE"].astype(str)
    surf["idINSPIRE"] = surf["idINSPIRE"].astype(str)

    # Generate geometry if absent (safety)
    if "geometry" not in sirene.columns or sirene.geometry.is_empty.any():
        sirene["geometry"] = sirene.apply(lambda row: Point(row["longitude"], row["latitude"]), axis=1)
        sirene = gpd.GeoDataFrame(sirene, geometry="geometry", crs="EPSG:2154")

    # Filter establishments corresponding to sales (NAF 47)
    sirene["naf2"] = sirene["activitePrincipaleEtablissement"].astype(str).str[:2]
    commerces = sirene[sirene["naf2"] == "47"]

    # Spatial join between shops and grid cells
    print_status("Join shops → grid", "info")
    joint = gpd.sjoin(grid, commerces, how="left", predicate="contains")
    count = joint.groupby("idINSPIRE").size().reset_index(name="nb_commerces")
    count["idINSPIRE"] = count["idINSPIRE"].astype(str)

    # Compute density by built surface
    print_status("Compute density", "info")
    merged = grid[["idINSPIRE"]].merge(count, on="idINSPIRE", how="left").merge(surf, on="idINSPIRE", how="left")
    merged = merged.fillna(0)
    merged = merged[merged["surf_batie"] > 10]  # avoid unstable divisions
    merged["densite_commerces"] = merged["nb_commerces"] / merged["surf_batie"]

    # Export the result
    merged[["idINSPIRE", "densite_commerces"]].to_csv(OUTPUT_PATH, index=False)
    print_status("Export completed", "ok", str(OUTPUT_PATH))


# Entry point
if __name__ == "__main__":
    compute_densite_commerces()
