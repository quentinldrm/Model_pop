"""
Script : part_pop_act.py
Objective : Compute the proportion of active population (18-64 years old) in each 200m grid cell,
            based on INSEE tiles weighted by population.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Feature generation step in the features pipeline.
"""

import geopandas as gpd
import pandas as pd
import os
from modele.scripts.features.features_utils import print_status

# === PATHS ===
GRID_PATH = "modele/output/grid/grid_mobiliscope_200m.parquet"
RECENS_PATH = "modele/data/raw/recens.parquet"
OUTPUT_PATH = "modele/output/features/part_actifs_200m.csv"


# Main function
def compute_part_population_active(grid: gpd.GeoDataFrame, recens: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        print_status("Computing proportion of active population per 200m grid cell", "info")

        # Age class variables considered as active
        actif_vars = ["ind_18_24", "ind_25_39", "ind_40_54", "ind_55_64"]
        all_vars = actif_vars + ["ind"]

        # Convert to numeric
        recens[all_vars] = recens[all_vars].apply(pd.to_numeric, errors="coerce")

        # Compute the proportion of active population in each tile
        recens["sum_actifs"] = recens[actif_vars].sum(axis=1)
        recens["part_actifs_200m"] = recens["sum_actifs"] / recens["ind"]
        recens = recens[recens["ind"] > 0].copy()

        # Spatial join to the grid
        recens = gpd.sjoin(recens, grid[["idINSPIRE", "geometry"]], how="inner", predicate="intersects")

        # Weighted average: part_actifs × ind
        recens["prod"] = recens["part_actifs_200m"] * recens["ind"]
        grouped = recens.groupby("idINSPIRE").agg(
            prod=("prod", "sum"),
            ind=("ind", "sum")
        ).reset_index()
        grouped["part_actifs"] = grouped["prod"] / grouped["ind"]

        print_status("Proportion of active population computed", "ok", f"{len(grouped)} grid cells")
        return grouped[["idINSPIRE", "part_actifs"]]

    except Exception as e:
        print_status("Error computing part_actifs", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "part_actifs"])


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

    result = compute_part_population_active(grid, recens)
    result.to_csv(OUTPUT_PATH, index=False)
    print_status("Export completed", "ok", OUTPUT_PATH)
