"""
Script : feature_d_moy_bati.py
Objective : Compute the average distance between buildings within each grid cell.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for generating spatial features related to building distances.
"""

import geopandas as gpd  
import pandas as pd      
import numpy as np       
from scipy.spatial.distance import pdist  
from appli.scripts.features.features_utils import load_config, print_status  

def compute_distance_moyenne_batiments(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        config = load_config()  # Load project configuration
        departement = config["departement"]
        path_bati = "appli/data/topo/BATIMENT.shp"  # Path to the building file

        # Load buildings and reproject to EPSG:2154
        bati = gpd.read_file(path_bati).to_crs("EPSG:2154")
        # Keep only polygons (buildings)
        bati = bati[bati.geometry.type == "Polygon"]
        # Compute the centroid of each building
        bati["centroid"] = bati.geometry.centroid
        # Replace geometry with centroid for spatial join
        bati = gpd.GeoDataFrame(bati, geometry="centroid", crs="EPSG:2154")

        # Spatial join: associate each building with a grid cell
        joined = gpd.sjoin(bati, grid, how="inner", predicate="within")

        # Compute average distances between buildings for each grid cell
        results = []
        for id_, group in joined.groupby("idINSPIRE"):
            # Retrieve coordinates of building centroids within the grid cell
            coords = np.array([[pt.x, pt.y] for pt in group.geometry])
            if len(coords) > 1:
                # Compute the average distance between all buildings in the grid cell
                dist = pdist(coords).mean()
                results.append((id_, dist))
            else:
                # If only one building, distance is undefined (NaN)
                results.append((id_, np.nan))

        # Create a DataFrame with the results
        df = pd.DataFrame(results, columns=["idINSPIRE", "distance_moyenne_batiments"])
        return df

    except Exception as e:
        # In case of error, display a message and return an empty DataFrame
        print_status("Error computing average building distances", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "distance_moyenne_batiments"])

if __name__ == "__main__":
    config = load_config()  # Load project configuration
    maillage = config["maillage"]
    departement = config["departement"]
    # Path to the spatial grid
    grid_path = f"appli/output/grid/grid_{departement}_{maillage}m.geojson"
    # Load the grid and reproject to EPSG:2154
    grid = gpd.read_file(grid_path).to_crs("EPSG:2154")

    print_status("Computing average building distances", "info")
    # Compute the average distance between buildings for each grid cell
    result = compute_distance_moyenne_batiments(grid)
    # Path to the output CSV file
    output_path = f"appli/output/features/distance_moyenne_batiments_{maillage}m.csv"
    # Export the results
    result.to_csv(output_path, index=False)
    print_status("Average building distances exported", "ok", output_path)
