"""
Script : dis_moy_bati.py
Objective : Compute the average distance between buildings (via centroids) in each 200m grid cell.
            Based on Euclidean distances in 2D with parallelization.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Feature generation step in the features pipeline.
"""

import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import box
from scipy.spatial.distance import pdist
from multiprocessing import cpu_count
from tqdm.contrib.concurrent import process_map
from modele.scripts.features.features_utils import print_status

# Auxiliary function called in parallel
def compute_mean_distance(group):
    id_, df = group
    coords = np.array([[pt.x, pt.y] for pt in df.geometry])
    if len(coords) > 1:
        return (id_, pdist(coords).mean())  # Average pairwise distances
    else:
        return (id_, np.nan)


# Main function: average distance between buildings per grid cell
def compute_distance_moyenne_batiments(grid: gpd.GeoDataFrame, bati: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        # Step 1: Spatial filtering with buffer
        bounds = grid.total_bounds
        minx, miny, maxx, maxy = bounds
        bbox = box(minx, miny, maxx, maxy).buffer(2000)
        bati = bati[bati.intersects(bbox)]
        bati = bati[bati.geometry.type.isin(["Polygon", "MultiPolygon"])]
        print_status(f"Number of buildings after filtering: {len(bati)}", "info")

        # Step 2: Compute centroids
        bati["centroid"] = bati.geometry.centroid
        bati = gpd.GeoDataFrame(bati, geometry="centroid", crs="EPSG:2154")

        # Step 3: Spatial join
        bati = bati.drop(columns=["index_right"], errors="ignore")
        grid = grid.drop(columns=["index_right"], errors="ignore")
        joined = gpd.sjoin(bati, grid, how="inner", predicate="within").drop(columns=["index_right"], errors="ignore")

        # Step 4: Compute average distances with parallelization
        print_status("Computing average distances", "info")
        grouped = list(joined.groupby("idINSPIRE"))
        results = process_map(
            compute_mean_distance,
            grouped,
            max_workers=cpu_count(),
            chunksize=1,
            desc="Computing distances"
        )

        return pd.DataFrame(results, columns=["idINSPIRE", "distance_moyenne_batiments"])

    except Exception as e:
        print_status("Error computing distance_moyenne_batiments", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "distance_moyenne_batiments"])


# Entry point (direct executable for tests outside the pipeline)
if __name__ == "__main__":
    grid_path = "modele/output/grid/grid_mobiliscope_200m.parquet"
    bati_path = "modele/data/processed/BATIMENT.parquet"
    output_path = "modele/output/features/distance_moyenne_batiments_200m.csv"

    grid = gpd.read_parquet(grid_path).to_crs("EPSG:2154")
    bati = gpd.read_parquet(bati_path).to_crs("EPSG:2154")

    print_status("Starting optimized computation with tqdm", "info")
    result = compute_distance_moyenne_batiments(grid, bati)

    result.to_csv(output_path, index=False)
    print_status("Export completed", "ok", output_path)
