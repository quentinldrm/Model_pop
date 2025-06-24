"""
Script : features_fusion.py
Objective : Merge all feature files for a given grid size into a single CSV file.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for combining spatial features into a unified dataset.
"""

import os
import pandas as pd
from appli.scripts.features.features_utils import load_config, print_status

def fusion_features():
    config = load_config()  # Load project configuration
    maillage = config["maillage"]
    feature_dir = "appli/output/features"

    print("=== MERGING FEATURES ===")
    # Detect all feature files matching the grid size
    all_files = [f for f in os.listdir(feature_dir) if f.endswith(f"_{maillage}m.csv")]
    print_status("Files detected", "info", f"{len(all_files)} files found")

    merged = None
    for file in all_files:
        path = os.path.join(feature_dir, file)
        df = pd.read_csv(path)

        # Merge files on the idINSPIRE column
        if merged is None:
            merged = df
        else:
            merged = pd.merge(merged, df, on="idINSPIRE", how="outer")

    if merged is not None:
        # Export the merged file
        output = f"appli/output/fusion/features_fusionnees_{maillage}m.csv"
        merged.to_csv(output, index=False)
        print_status("Merged file exported", "ok", output)
        print("=== MERGING COMPLETED ===")
    else:
        print_status("No files to merge", "warning", "No matching files found.")
    

if __name__ == "__main__":
    fusion_features()
