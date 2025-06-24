"""
Script : vol_moy.py
Objective : Compute the average built volume per 200m grid cell,
            using the surface area and height of buildings (volume = surface × height),
            and weighting the average by the built surface area.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Feature generation step in the feature pipeline.
"""

import geopandas as gpd
import pandas as pd
import os
from appli.scripts.features.features_utils import print_status, load_config

config = load_config()
departement = config["departement"]
maillage = config["maillage"]

# === PATHS ===
BATI_PATH = "appli/data/topo/BATIMENT.shp"
GRID_PATH = f"appli/output/grid/grid_{departement}_{maillage}m.geojson"
OUTPUT_PATH = f"appli/output/features/volume_moyen_bati_{maillage}m.csv"


# Main function
def compute_volume_moyen_par_maille(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        # Check if the building file exists
        if not os.path.exists(BATI_PATH):
            print_status("BATIMENT.shp file not found", "err", BATI_PATH)
            return pd.DataFrame(columns=["idINSPIRE", "volume_moyen_bati"])

        # Load and clean data
        bati = gpd.read_file(BATI_PATH).to_crs("EPSG:2154")
        bati = bati[bati.geometry.type.isin(["Polygon", "MultiPolygon"])]  # Keep polygons and multipolygons
        bati["geometry"] = bati["geometry"].buffer(0)  # Fix invalid geometries
        bati = bati[bati["HAUTEUR"].notna()]  # Filter buildings with valid height data
        bati["hauteur"] = pd.to_numeric(bati["HAUTEUR"], errors="coerce")
        bati["surface"] = bati.geometry.area  # Compute surface area
        bati["volume"] = bati["surface"] * bati["hauteur"]  # Compute volume
        bati = bati.dropna(subset=["volume", "surface"])  # Remove invalid rows
        bati = bati[bati["surface"] > 0]  # Filter buildings with positive surface area

        # Clean unnecessary columns
        bati.drop(columns=["index_right"], inplace=True, errors="ignore")
        grid.drop(columns=["index_right"], inplace=True, errors="ignore")

        # Spatial join: assign buildings to grid cells
        joined = gpd.sjoin(bati, grid, how="inner", predicate="intersects")

        # Weighted aggregation: sum(volume * surface) / sum(surface)
        joined["prod"] = joined["volume"] * joined["surface"]
        grouped = joined.groupby("idINSPIRE").agg(
            somme_volume=("prod", "sum"),
            somme_surface=("surface", "sum")
        ).reset_index()
        grouped["volume_moyen_bati"] = grouped["somme_volume"] / grouped["somme_surface"]

        return grouped[["idINSPIRE", "volume_moyen_bati"]]

    except Exception as e:
        # Handle errors and return an empty DataFrame
        print_status("Error computing average built volume", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "volume_moyen_bati"])


# Direct execution
if __name__ == "__main__":
    print_status("Computing average built volume", "info")

    grid = gpd.read_file(GRID_PATH).to_crs("EPSG:2154")
    result = compute_volume_moyen_par_maille(grid)
    result.to_csv(OUTPUT_PATH, index=False)

    print_status("Average built volume exported", "ok", OUTPUT_PATH)
