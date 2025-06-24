"""
Script : feature_emp_est.py
Objective : Estimate the number of jobs within each grid cell based on SIRENE data,
            using weighted averages for establishments with missing data.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for generating spatial features related to job estimation.
"""

import geopandas as gpd
import pandas as pd
import os
from appli.scripts.features.features_utils import load_config, print_status

def compute_emplois_estimes_pondere(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        config = load_config()  # Load project configuration
        departement = config["departement"]
        path = f"appli/data/sirene/sirene.geojson"

        # Check if the SIRENE file exists
        if not os.path.exists(path):
            print_status("SIRENE file not found", "err")
            return pd.DataFrame(columns=["idINSPIRE", "emplois_estimes_pondere"])

        gdf = gpd.read_file(path)

        # Reconstruct geometry from Lambert coordinates if geometry is null
        gdf["x"] = pd.to_numeric(gdf["longitude"], errors="coerce")
        gdf["y"] = pd.to_numeric(gdf["latitude"], errors="coerce")
        gdf = gdf.dropna(subset=["x", "y"])
        gdf["geometry"] = gpd.points_from_xy(gdf["x"], gdf["y"])
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs="EPSG:2154")

        # Dictionary mapping tranche codes to average employee counts
        TRANCHE_TO_EFFECTIF = {
            "00": 0, "01": 1.5, "02": 4, "03": 8, "11": 15,
            "12": 35, "21": 75, "22": 150, "31": 225, "32": 350,
            "41": 500, "42": 750, "51": 1000, "52": 1500, "53": 2000
        }

        gdf["tranche"] = gdf["trancheEffectifsEtablissement"].map(TRANCHE_TO_EFFECTIF)
        gdf["naf2"] = gdf["activitePrincipaleEtablissement"].astype(str).str[:2]

        # Compute fallback average employee counts by NAF code
        naf_fallback = gdf.dropna(subset=["tranche"]).groupby("naf2")["tranche"].mean()

        # Estimate the number of employees for each establishment
        gdf["emplois_estimes"] = gdf.apply(
            lambda row: row["tranche"]
            if pd.notnull(row["tranche"])
            else naf_fallback.get(row["naf2"], 0),
            axis=1
        )

        # Spatial join: assign establishments to grid cells
        joined = gpd.sjoin(grid, gdf[["geometry", "emplois_estimes"]], how="left", predicate="contains")
        emplois = joined.groupby("idINSPIRE")["emplois_estimes"].sum().reset_index()
        emplois.rename(columns={"emplois_estimes": "emplois_estimes_pondere"}, inplace=True)

        return emplois

    except Exception as e:
        # Handle errors and return an empty DataFrame
        print_status("Error computing weighted job estimates", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "emplois_estimes_pondere"])

if __name__ == "__main__":
    config = load_config()  # Load project configuration
    maillage = config["maillage"]
    departement = config["departement"]
    grid_path = f"appli/output/grid/grid_{departement}_{maillage}m.geojson"
    grid = gpd.read_file(grid_path).to_crs("EPSG:2154")

    print_status("Computing weighted job estimates", "info")
    result = compute_emplois_estimes_pondere(grid)
    output_path = f"appli/output/features/emplois_estimes_pondere_{maillage}m.csv"
    result.to_csv(output_path, index=False)
    print_status("Job estimates exported", "ok", output_path)
