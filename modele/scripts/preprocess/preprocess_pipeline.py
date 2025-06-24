"""
Script : preprocess_pipeline.py
Objective : Orchestrate all spatial preprocessing steps before feature generation,
            including sector merging, grid generation, built surface area calculation,
            and complete processing of Mobiliscope population data.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Master script for the preprocessing module.
"""

from scripts.preprocess.secteurs import merge_geojson_files
from scripts.preprocess.create_grid_secteurs import main as create_grid
from scripts.preprocess.area_bati_secteurs import calculate_surface_batie_optimisee
from scripts.preprocess.pop_secteur_fusion import main as process_mobiliscope
from scripts.preprocess.preprocess_utils import print_status


# Execute a function with explicit logging
def safe_run(name, func):
    try:
        print_status(f"Starting: {name}", "info")
        func()
        print_status(f"{name} completed", "ok")
    except Exception as e:
        print_status(f"{name} failed", "err", str(e))


# Main function: chaining modules
def main():
    print_status("=== STARTING PREPROCESSING PIPELINE ===", "info")

    safe_run("Mobiliscope sector merging", merge_geojson_files)
    safe_run("200m grid creation", create_grid)
    safe_run("Built surface area calculation per grid cell", calculate_surface_batie_optimisee)
    safe_run("Mobiliscope population processing (day/night)", process_mobiliscope)

    print_status("=== PREPROCESSING PIPELINE COMPLETED ===", "ok")


if __name__ == "__main__":
    main()
