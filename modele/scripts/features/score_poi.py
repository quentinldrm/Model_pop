"""
Script : score_poi.py
Objective : Compute an attractiveness score based on Points of Interest (POI)
            by weighting each type with a coefficient from a catalog.
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
OUTPUT_PATH = "modele/output/features/score_poi_pondere_200m.csv"
CATALOGUE_PATH = "modele/utils/score_POI.csv"
POI_PATHS = {
    "amenity": "modele/data/raw/amenity.parquet",
    "shop": "modele/data/raw/shop.parquet",
    "office": "modele/data/raw/office.parquet",
    "leisure": "modele/data/raw/leisure.parquet",
}


# Main function
def compute_score_poi_pondere(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        # Read the weighting catalog
        weights_df = pd.read_csv(CATALOGUE_PATH, sep=";")
        weights = dict(zip(weights_df["poi"], weights_df["score"]))

        all_poi = []

        for tag, path in POI_PATHS.items():
            if not os.path.exists(path):
                print_status(f"{tag}.parquet missing", "err")
                continue

            gdf = gpd.read_parquet(path).to_crs("EPSG:2154")

            if tag not in gdf.columns:
                print_status(f"Field {tag} missing in {path}", "err")
                continue

            gdf["type_poi"] = gdf[tag]
            gdf["poids"] = gdf["type_poi"].map(weights).fillna(0)
            all_poi.append(gdf[["geometry", "poids"]])

        if not all_poi:
            print_status("No valid POI loaded", "err")
            return pd.DataFrame(columns=["idINSPIRE", "score_poi_pondere"])

        # Combine all POIs
        poi_combined = pd.concat(all_poi, ignore_index=True)
        poi_combined = gpd.GeoDataFrame(poi_combined, geometry="geometry", crs="EPSG:2154")

        # Spatial join POI → grid
        print_status("Spatial join POI → grid", "info")
        joined = gpd.sjoin(grid, poi_combined, how="left", predicate="contains")
        joined.drop(columns=["index_right"], errors="ignore", inplace=True)

        # Aggregation
        scores = joined.groupby("idINSPIRE")["poids"].sum().reset_index()
        scores.rename(columns={"poids": "score_poi_pondere"}, inplace=True)

        return scores

    except Exception as e:
        print_status("Error computing score_poi_pondere", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "score_poi_pondere"])


# Direct execution
if __name__ == "__main__":
    print_status("Computing score_poi_pondere", "info")

    grid = gpd.read_parquet(GRID_PATH).to_crs("EPSG:2154")
    grid.drop(columns=["index_right"], errors="ignore", inplace=True)

    result = compute_score_poi_pondere(grid)
    result.to_csv(OUTPUT_PATH, index=False)

    print_status("Weighted POI score exported", "ok", OUTPUT_PATH)
