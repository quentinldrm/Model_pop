"""
Script : preprocess_create_grid.py
Objective : Create a grid of square polygons for a given department and cell size.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Preprocessing step for generating spatial grids in the modeling pipeline.
"""

import geopandas as gpd  
import numpy as np       
import yaml
import os               
from shapely.geometry import box  
from appli.scripts.preprocess.preprocess_utils import load_config, print_status 

# Create a grid of square polygons
def create_grid(bounds, cell_size):
    # bounds : (xmin, ymin, xmax, ymax)
    xmin, ymin, xmax, ymax = bounds
    # Generate coordinates for rows and columns of the grid
    rows = np.arange(ymin, ymax, cell_size)
    cols = np.arange(xmin, xmax, cell_size)
    # Create a list of square polygons for each grid cell
    polygons = [box(x, y, x + cell_size, y + cell_size) for x in cols for y in rows]
    # Return a GeoDataFrame with all grid cells
    return gpd.GeoDataFrame(geometry=polygons, crs="EPSG:2154")

def main():
    config = load_config()  # Load project configuration
    departement = config["departement"]
    cell_size = int(config["maillage"])

    print_status("Creating grid", "info", f"{cell_size}m")

    try:
        # Load department geometry from an online GeoJSON file
        dep = gpd.read_file("https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/departements-version-simplifiee.geojson")
        dep = dep[dep["code"] == departement].to_crs("EPSG:2154")
        if dep.empty:
            raise ValueError("Department not found.")

        # Retrieve department bounds to create the grid
        bounds = dep.total_bounds
        grid = create_grid(bounds, cell_size)
        # Keep only cells that intersect the department
        grid = grid[grid.intersects(dep.geometry.squeeze())]
        # Assign a unique identifier to each grid cell
        grid["idINSPIRE"] = grid.index.astype(str)

        # Create the output directory if needed
        os.makedirs("appli/output/grid", exist_ok=True)
        output = f"appli/output/grid/grid_{departement}_{cell_size}m.geojson"
        # Save the grid as a GeoJSON file
        grid.to_file(output, driver="GeoJSON")
        print_status("Grid saved", "ok", output)

    except Exception as e:
        # Display an error message in case of issues
        print_status("Grid creation failed", "err", str(e))

if __name__ == "__main__":
    main()
