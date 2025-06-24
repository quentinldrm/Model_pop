"""
Script : feature_poi.py
Objective : Compute the weighted POI (Point of Interest) score within each grid cell,
            based on OSM data and a predefined scoring catalog.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for generating spatial features related to POI density and importance.
"""

import geopandas as gpd
import pandas as pd
import os
from appli.scripts.features.features_utils import load_config, print_status

def compute_score_poi_pondere(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        config = load_config()  # Load project configuration
        base_dir = "appli/data/osm"
        files = {
            "amenity": os.path.join(base_dir, "amenity.geojson"),
            "shop": os.path.join(base_dir, "shop.geojson"),
            "office": os.path.join(base_dir, "office.geojson"),
            "leisure": os.path.join(base_dir, "leisure.geojson"),
        }

        # Load the scoring catalog
        weights_df = pd.read_csv("appli/data/features/score_POI.csv", sep=";")
        weights = dict(zip(weights_df["poi"], weights_df["score"]))

        all_poi = []

        # Load POI files and assign weights
        for tag, path in files.items():
            if not os.path.exists(path):
                print_status(f"{tag}.geojson missing", "err")
                continue
            gdf = gpd.read_file(path).to_crs("EPSG:2154")
            if tag not in gdf.columns:
                print_status(f"Field {tag} missing in {path}", "err")
                continue
            gdf["type_poi"] = gdf[tag]
            gdf["poids"] = gdf["type_poi"].map(weights).fillna(0)  # Map weights to POI types
            all_poi.append(gdf[["geometry", "poids"]])

        # Check if any valid POI data was loaded
        if not all_poi:
            print_status("No valid POI loaded", "err")
            return pd.DataFrame(columns=["idINSPIRE", "score_poi_pondere"])

        # Combine all POI data into a single GeoDataFrame
        poi_combined = pd.concat(all_poi, ignore_index=True)
        poi_combined = gpd.GeoDataFrame(poi_combined, geometry="geometry", crs="EPSG:2154")

        # Spatial join with the grid
        joined = gpd.sjoin(grid, poi_combined, how="left", predicate="contains")

        # Aggregate scores by grid cell
        scores = joined.groupby("idINSPIRE")["poids"].sum().reset_index()
        scores.rename(columns={"poids": "score_poi_pondere"}, inplace=True)

        return scores

    except Exception as e:
        # Handle errors and return an empty DataFrame
        print_status("Error computing weighted POI score", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "score_poi_pondere"])

if __name__ == "__main__":
    config = load_config()  # Load project configuration
    maillage = config["maillage"]
    departement = config["departement"]
    grid_path = f"appli/output/grid/grid_{departement}_{maillage}m.geojson"
    grid = gpd.read_file(grid_path).to_crs("EPSG:2154")

    print_status("Computing weighted POI score", "info")
    result = compute_score_poi_pondere(grid)
    output_path = f"appli/output/features/score_poi_pondere_{maillage}m.csv"
    result.to_csv(output_path, index=False)
    print_status("Weighted POI score exported", "ok", output_path)
