"""
Script : dens_eta.py
Objective : Compute the density of SIRENE establishments (all sectors combined)
            by dividing the number of establishments by the built surface area in each grid cell.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Feature generation step in the features pipeline.
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from modele.scripts.features.features_utils import load_config, print_status

# === SCRIPT PARAMETERS ===
GRID_PATH = "modele/output/grid/grid_mobiliscope_200m.parquet"
SURF_PATH = "modele/data/processed/surf_bati_200m.csv"
SIRENE_PATH = "modele/data/raw/sirene.parquet"
OUTPUT_PATH = "modele/output/features/densite_etablissements_200m.csv"


# Main function: computes the density of SIRENE establishments
def compute_densite_etablissements():
    print_status("Loading files", "info")

    # Read files
    grid = gpd.read_parquet(GRID_PATH).to_crs("EPSG:2154")
    surf = pd.read_csv(SURF_PATH)
    sirene = gpd.read_parquet(SIRENE_PATH).to_crs("EPSG:2154")

    # Clean inherited columns
    for df in [grid, surf]:
        if "index_right" in df.columns:
            df.drop(columns=["index_right"], inplace=True)

    # Verify or reconstruct missing geometry
    if "geometry" not in sirene.columns or sirene.geometry.is_empty.any():
        sirene["x"] = pd.to_numeric(sirene["longitude"], errors="coerce")
        sirene["y"] = pd.to_numeric(sirene["latitude"], errors="coerce")
        sirene = sirene.dropna(subset=["x", "y"])
        sirene["geometry"] = [Point(xy) for xy in zip(sirene["x"], sirene["y"])]
        sirene = gpd.GeoDataFrame(sirene, geometry="geometry", crs="EPSG:2154")

    # Harmonize identifiers
    grid["idINSPIRE"] = grid["idINSPIRE"].astype(str)
    surf["idINSPIRE"] = surf["idINSPIRE"].astype(str)

    # Spatial join: attach establishments to grid cells
    print_status("Spatial join SIRENE → grid", "info")
    joined = gpd.sjoin(grid, sirene, how="left", predicate="contains")
    count = joined.groupby("idINSPIRE").size().reset_index(name="nb_etabs_sirene")
    count["idINSPIRE"] = count["idINSPIRE"].astype(str)

    # Merge with built surface area
    print_status("Merge with built surface area", "info")
    df = grid[["idINSPIRE"]].merge(count, on="idINSPIRE", how="left").merge(surf, on="idINSPIRE", how="left")
    df = df.fillna(0)
    df = df[df["surf_batie"] > 10]  # safety filter

    # Final computation
    print_status("Compute densities", "info")
    df["densite_etablissements"] = df["nb_etabs_sirene"] / df["surf_batie"]

    # Export
    df[["idINSPIRE", "densite_etablissements"]].to_csv(OUTPUT_PATH, index=False)
    print_status("Export completed", "ok", str(OUTPUT_PATH))


# Entry point
if __name__ == "__main__":
    compute_densite_etablissements()
