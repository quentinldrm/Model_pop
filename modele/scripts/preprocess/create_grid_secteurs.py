"""
Script : create_grid_secteurs.py
Objective : Generate a regular 200m by 200m grid, restricted to areas covered by Mobiliscope sectors,
            using a spatial join (optional Dask).
Author : LEDERMANN Quentin
Date : June 2025
Usage : Preprocessing script, used to create a spatial grid before feature generation.
"""

import geopandas as gpd
import numpy as np
import os
from shapely.geometry import box
from geopandas.tools import sjoin
from typing import Optional, Literal

# OPTION: enable Dask usage (useful for very large sectors)
USE_DASK = True

# === SCRIPT PARAMETERS ===
CELL_SIZE = 200
SECTEURS_PATH = "modele/data/processed/secteurs.parquet"
OUTPUT_DIR = "modele/output/grid"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, f"grid_mobiliscope_{CELL_SIZE}m.parquet")


# Save a GeoDataFrame in compressed GeoParquet format
def save_geoparquet(
    gdf: gpd.GeoDataFrame,
    path: str,
    compression: Optional[Literal["snappy", "gzip", "brotli"]] = "brotli"
):
    gdf.to_parquet(path, compression=compression, index=False)


# Create a regular grid (tiling) from bounds and cell size
def create_grid(bounds, cell_size):
    xmin, ymin, xmax, ymax = bounds
    rows = np.arange(ymin, ymax, cell_size)
    cols = np.arange(xmin, xmax, cell_size)
    polygons = [box(x, y, x + cell_size, y + cell_size) for x in cols for y in rows]
    return gpd.GeoDataFrame(geometry=polygons, crs="EPSG:2154")


# Main function: grid creation and export
def main():
    print(f"[→] Starting grid creation {CELL_SIZE}m")

    try:
        # Load study sectors
        if not os.path.exists(SECTEURS_PATH):
            raise FileNotFoundError(f"File not found: {SECTEURS_PATH}")

        secteurs = gpd.read_parquet(SECTEURS_PATH).to_crs("EPSG:2154")
        if secteurs.empty:
            raise ValueError("Sector file is empty.")

        # Compute bounds with a buffer (to smooth edges)
        bounds = secteurs.buffer(2000).total_bounds
        print(f"[→] Bounds used: {bounds}")

        # Generate the complete grid
        grid = create_grid(bounds, CELL_SIZE)
        print(f"[→] Initial grid: {len(grid)} cells")

        # Simplify geometries to speed up calculations
        secteurs["geometry"] = secteurs["geometry"].simplify(tolerance=5)

        # Spatial join (with or without Dask)
        if USE_DASK:
            import dask_geopandas as dgpd
            grid = dgpd.from_geopandas(grid, npartitions=4)
            secteurs = dgpd.from_geopandas(secteurs, npartitions=4)
            grid = grid.sjoin(secteurs, predicate="intersects")
            grid = grid.compute()
        else:
            grid = sjoin(grid, secteurs, predicate="intersects", how="inner").drop(columns="index_right")

        print(f"[→] Grid after intersection: {len(grid)} cells")

        # Add a unique identifier
        grid["idINSPIRE"] = grid.index.astype(str)

        # Save
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        save_geoparquet(grid, OUTPUT_PATH)
        print(f"[✓] Grid saved: {OUTPUT_PATH}")

    except Exception as e:
        print(f"[✗] Error: {str(e)}")


# Entry point
if __name__ == "__main__":
    main()
