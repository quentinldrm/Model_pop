"""
Script : preprocess_pipeline.py
Objective : Execute the preprocessing pipeline, including grid creation, census data adaptation, 
            and built surface area calculation.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Master script for preprocessing steps in the modeling pipeline.
"""

from scripts.preprocess.preprocess_create_grid import main as create_grid
from scripts.preprocess.preprocess_adapt_recens import adapt_recens_to_maillage
from scripts.preprocess.preprocess_area_bati import compute_surface_batie
from scripts.preprocess.preprocess_utils import print_status

def safe_run(name, func):
    # Execute a function with explicit logging
    try:
        print_status(f"Starting {name}", "info")
        func()
        print_status(f"{name} completed", "ok")
    except Exception as e:
        print_status(f"{name} failed", "err", str(e))

def main():
    print("=== STARTING PREPROCESSING PIPELINE ===")

    # Execute preprocessing steps sequentially
    safe_run("create_grid", create_grid)
    safe_run("adapt_recens_maillage", adapt_recens_to_maillage)
    safe_run("compute_surface_batie", compute_surface_batie)

    print("=== PREPROCESSING PIPELINE COMPLETED ===")

if __name__ == "__main__":
    main()
