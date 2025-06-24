"""
Script : emp_est.py
Objective : Estimate the number of jobs per 200m grid cell by intersecting the grid with the SIRENE database,
            using weighted employee ranges (and a fallback by NAF code).
Author : LEDERMANN Quentin
Date : June 2025
Usage : Feature generation step in the features pipeline.
"""

import os
import pandas as pd
import geopandas as gpd
from modele.scripts.features.features_utils import print_status

# === SCRIPT PARAMETERS ===
SIRENE_PATH = "modele/data/raw/sirene.parquet"
GRID_PATH = "modele/output/grid/grid_mobiliscope_200m.parquet"
OUTPUT_PATH = "modele/output/features/emplois_estimes_pondere_200m.csv"


# Main function
def compute_emplois_estimes_pondere(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        if not os.path.exists(SIRENE_PATH):
            print_status("SIRENE file not found", "err")
            return pd.DataFrame(columns=["idINSPIRE", "emplois_estimes_pondere"])

        gdf = gpd.read_parquet(SIRENE_PATH)
        print_status("SIRENE loaded", "ok", f"{len(gdf)} establishments")

        # Reconstruct geometry if necessary
        if "geometry" not in gdf.columns or gdf.geometry.is_empty.all():
            print_status("Reconstructing geometry from Lambert coordinates", "warn")
            gdf["x"] = pd.to_numeric(gdf["coordonneeLambertAbscisseEtablissement"], errors="coerce")
            gdf["y"] = pd.to_numeric(gdf["coordonneeLambertOrdonneeEtablissement"], errors="coerce")
            gdf = gdf.dropna(subset=["x", "y"])
            gdf["geometry"] = gpd.points_from_xy(gdf["x"], gdf["y"])
            gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs="EPSG:2154")
        else:
            gdf = gdf.set_crs("EPSG:2154", allow_override=True)

        # Cleanup
        for df in [grid, gdf]:
            if "index_right" in df.columns:
                df.drop(columns=["index_right"], inplace=True)

        # Spatial filtering
        gdf = gpd.sjoin(gdf, grid[["geometry"]], how="inner", predicate="intersects")
        print_status("SIRENE spatially filtered", "info", f"{len(gdf)} establishments")

        # Map employee ranges to estimated averages
        tranche_map = {
            "00": 0, "01": 1.5, "02": 4, "03": 8, "11": 15, "12": 35,
            "21": 75, "22": 150, "31": 225, "32": 350, "41": 500,
            "42": 750, "51": 1000, "52": 1500, "53": 2000
        }
        gdf["tranche"] = gdf["trancheEffectifsEtablissement"].map(tranche_map)
        gdf["naf2"] = gdf["activitePrincipaleEtablissement"].astype(str).str[:2]

        # Fallback average by NAF code
        naf_fallback = gdf.dropna(subset=["tranche"]).groupby("naf2")["tranche"].mean()

        # Fill: tranche → fallback NAF → 0
        gdf["emplois_estimes"] = gdf.apply(
            lambda row: row["tranche"] if pd.notnull(row["tranche"]) else naf_fallback.get(row["naf2"], 0),
            axis=1
        )

        # Grid → job join
        joined = gpd.sjoin(grid, gdf[["geometry", "emplois_estimes"]], how="left", predicate="intersects")

        # Aggregate by grid cell
        emplois = joined.groupby("idINSPIRE")["emplois_estimes"].sum().reset_index()
        emplois.rename(columns={"emplois_estimes": "emplois_estimes_pondere"}, inplace=True)

        return emplois

    except Exception as e:
        print_status("Error computing emplois_estimes_pondere", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "emplois_estimes_pondere"])


# Direct execution
if __name__ == "__main__":
    print_status("Computing emplois_estimes_pondere", "info")

    grid = gpd.read_parquet(GRID_PATH)

    # CRS safety
    if grid.crs is None:
        print_status("Grid CRS missing — forcing EPSG:2154", "warn")
        grid.set_crs("EPSG:2154", inplace=True)
    elif grid.crs != "EPSG:2154":
        print_status("Forcing grid reprojection", "warn")
        grid = grid.to_crs("EPSG:2154")

    result = compute_emplois_estimes_pondere(grid)
    result.to_csv(OUTPUT_PATH, index=False)
    print_status("Job estimation exported", "ok", OUTPUT_PATH)
