"""
Script : vol_moy.py
Objective : Compute the average built volume per 200m grid cell,
            using the surface area and height of buildings (volume = surface × height),
            then weighting the average by the built surface area.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Feature generation step in the features pipeline.
"""

import geopandas as gpd
import pandas as pd
import os
from modele.scripts.features.features_utils import print_status

# === PATHS ===
BATI_PATH = "modele/data/processed/BATIMENT.parquet"
GRID_PATH = "modele/output/grid/grid_mobiliscope_200m.parquet"
OUTPUT_PATH = "modele/output/features/volume_moyen_bati_200m.csv"


# Main function
def compute_volume_moyen_par_maille(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        if not os.path.exists(BATI_PATH):
            print_status("BATIMENT.parquet file not found", "err", BATI_PATH)
            return pd.DataFrame(columns=["idINSPIRE", "volume_moyen_bati"])

        # Load and clean data
        bati = gpd.read_parquet(BATI_PATH).to_crs("EPSG:2154")
        bati = bati[bati.geometry.type.isin(["Polygon", "MultiPolygon"])]
        bati["geometry"] = bati["geometry"].buffer(0)
        bati = bati[bati["HAUTEUR"].notna()]
        bati["hauteur"] = pd.to_numeric(bati["HAUTEUR"], errors="coerce")
        bati["surface"] = bati.geometry.area
        bati["volume"] = bati["surface"] * bati["hauteur"]
        bati = bati.dropna(subset=["volume", "surface"])
        bati = bati[bati["surface"] > 0]

        # Clean unnecessary columns
        bati.drop(columns=["index_right"], inplace=True, errors="ignore")
        grid.drop(columns=["index_right"], inplace=True, errors="ignore")

        # Spatial join
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
        print_status("Error computing volume_moyen_bati", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "volume_moyen_bati"])


# Direct execution
if __name__ == "__main__":
    print_status("Computing volume_moyen_bati", "info")

    grid = gpd.read_parquet(GRID_PATH).to_crs("EPSG:2154")
    result = compute_volume_moyen_par_maille(grid)
    result.to_csv(OUTPUT_PATH, index=False)

    print_status("Average volume exported", "ok", OUTPUT_PATH)
