"""
Script : area_bati_secteurs.py
Objective : Compute the built surface area (m²) per 200m grid cell by intersecting building polygons
            with grid cells via an optimized spatial join.
Author : LEDERMANN Quentin
Date : June 2025
Usage : This script is called in the preprocessing pipeline to create the "built surface area" variable.
"""

import geopandas as gpd
import pandas as pd
from tqdm import tqdm
import os

# === SCRIPT PARAMETERS ===
PATH_GRID = "modele/output/grid/grid_mobiliscope_200m.parquet"
PATH_BATI = "modele/data/processed/BATIMENT.parquet"
OUTPUT_CSV = "modele/data/processed/surf_bati_200m.csv"


# Main spatial computation function
def calculate_surface_batie_optimisee():
    """
    Computes the built surface area intersecting each grid cell (200m) via a spatial join,
    with geometry processing and final CSV export.
    """

    # Load grid cells
    print("Loading grid...")
    grid = gpd.read_parquet(PATH_GRID).to_crs("EPSG:2154")

    # Create a unique identifier if missing
    if "idINSPIRE" not in grid.columns:
        grid["idINSPIRE"] = [f"cell_{i}" for i in range(len(grid))]

    # Load buildings
    print("Loading buildings...")
    bati = gpd.read_parquet(PATH_BATI).to_crs("EPSG:2154")
    bati = bati[bati.geometry.notnull()]  # Cleanup
    bati["area"] = bati.geometry.area     # Compute raw surface area (not used here)

    # Spatial join: associate each building with one or more grid cells
    print("Performing spatial join...")
    joined = gpd.sjoin(bati, grid[["idINSPIRE", "geometry"]], predicate="intersects", how="inner")

    # Retrieve grid cell geometries (for precise intersection)
    print("Retrieving grid cell geometries...")
    grid_geom = grid.drop_duplicates("idINSPIRE").set_index("idINSPIRE").geometry
    joined["grid_geom"] = joined["idINSPIRE"].map(grid_geom)

    # Compute intersection geometry between building and grid cell
    print("Computing intersections...")
    joined["intersection"] = joined.geometry.intersection(
        gpd.GeoSeries(joined["grid_geom"], index=joined.index)
    )
    joined["surf_inters"] = joined["intersection"].area

    # Aggregate: sum intersected areas per grid cell
    print("Aggregating areas by grid cell...")
    result = joined.groupby("idINSPIRE")["surf_inters"].sum().reset_index()
    result.columns = ["idINSPIRE", "surf_batie"]

    # Export to CSV
    print("Exporting CSV...")
    result.to_csv(OUTPUT_CSV, index=False)
    print(f"Export completed: {OUTPUT_CSV}")


# Entry point
if __name__ == "__main__":
    calculate_surface_batie_optimisee()
