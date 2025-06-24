"""
Script : preprocess_adapt_recens.py
Objective : Adapt census data (200m grid) to a user-defined grid size.
            Aggregate INSEE indicators per grid cell using spatial joins.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Preprocessing step for adapting census data to custom grids.
"""

import geopandas as gpd
import pandas as pd
from appli.scripts.features.features_utils import load_config, print_status

def adapt_recens_to_maillage():
    # Adapt census data to the user-defined grid size and aggregate indicators.

    # Load global parameters
    config = load_config()
    departement = config["departement"]
    maillage = int(config["maillage"])

    # Define input/output file paths
    path_recens = f"appli/data/recens/recens_{departement}.geojson"
    path_grid = f"appli/output/grid/grid_{departement}_{maillage}m.geojson"
    path_out = f"appli/data/features/recens_agrege_{maillage}m.csv"

    # Load INSEE data (200m grid cells)
    print_status("Loading census data", "info")
    recens = gpd.read_file(path_recens).to_crs("EPSG:2154")

    # Force conversion of all indicator columns to float
    cols_to_convert = [col for col in recens.columns if col.startswith(("ind", "log", "men"))]
    recens[cols_to_convert] = recens[cols_to_convert].apply(pd.to_numeric, errors="coerce")

    # Load the target grid
    print_status(f"Loading target grid {maillage}m", "info")
    grid = gpd.read_file(path_grid).to_crs("EPSG:2154")

    # Perform spatial join with predicate='intersects' to capture exact overlaps
    print_status("Performing spatial join between grid and census cells", "info")
    joint = gpd.sjoin(grid, recens, how="left", predicate="intersects")

    # Aggregate indicators per grid cell
    print_status("Aggregating INSEE indicators", "info")
    grouped = joint.groupby("idINSPIRE")[cols_to_convert].sum().reset_index()

    # Reinstate uncovered grid cells with null values replaced by 0
    print_status("Final merge with complete grid", "info")
    full = grid[["idINSPIRE"]].merge(grouped, on="idINSPIRE", how="left").fillna(0)

    # Export the aggregated data
    full.to_csv(path_out, index=False)
    print_status("Aggregated census file exported", "ok", path_out)

if __name__ == "__main__":
    adapt_recens_to_maillage()
