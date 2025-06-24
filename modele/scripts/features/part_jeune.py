"""
Script : part_jeune.py
Objective : Compute the proportion of young people (0 to 24 years old) in each 200m grid cell,
            based on disaggregated INSEE data and a weighted average.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Feature generation step in the features pipeline.
"""

import os
import pandas as pd
import geopandas as gpd
from modele.scripts.features.features_utils import print_status

# === PATHS ===
GRID_PATH = "modele/output/grid/grid_mobiliscope_200m.parquet"
RECENS_PATH = "modele/data/raw/recens.parquet"
OUTPUT_PATH = "modele/output/features/part_jeunes_200m.csv"


# Main function
def compute_part_jeunes(grid: gpd.GeoDataFrame, recens: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        print_status("Computing proportion of young people per 200m grid cell", "info")

        # Age variables to consider as young
        jeunes_vars = ["ind_0_3", "ind_4_5", "ind_6_10", "ind_11_17", "ind_18_24"]

        # Convert to float
        numeric_columns = jeunes_vars + ["ind"]
        recens[numeric_columns] = recens[numeric_columns].apply(pd.to_numeric, errors="coerce")

        # Partial calculation in each tile
        recens["sum_jeunes"] = recens[jeunes_vars].sum(axis=1)
        recens["part_jeunes_200m"] = recens["sum_jeunes"] / recens["ind"]
        recens = recens[recens["ind"] > 0].copy()

        # Spatial join to the grid
        recens = gpd.sjoin(recens, grid[["idINSPIRE", "geometry"]], how="inner", predicate="intersects")

        # Weighted average by total population
        recens["prod"] = recens["part_jeunes_200m"] * recens["ind"]
        grouped = recens.groupby("idINSPIRE").agg(
            prod=("prod", "sum"),
            ind=("ind", "sum")
        ).reset_index()
        grouped["part_jeunes"] = grouped["prod"] / grouped["ind"]

        print_status("Proportion of young people computed", "ok", f"{len(grouped)} grid cells")
        return grouped[["idINSPIRE", "part_jeunes"]]

    except Exception as e:
        print_status("Error computing part_jeunes", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "part_jeunes"])


# Direct execution
if __name__ == "__main__":
    print_status("Loading data", "info")

    grid = gpd.read_parquet(GRID_PATH)
    recens = gpd.read_parquet(RECENS_PATH)

    # Harmonize CRS
    if grid.crs != "EPSG:2154":
        grid = grid.to_crs("EPSG:2154")
    if recens.crs != "EPSG:2154":
        recens = recens.to_crs("EPSG:2154")

    result = compute_part_jeunes(grid, recens)
    result.to_csv(OUTPUT_PATH, index=False)
    print_status("Export completed", "ok", OUTPUT_PATH)
