"""
Script : feature_jeune.py
Objective : Compute the proportion of young people within each grid cell based on aggregated census data.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for generating spatial features related to the population of young people.
"""

import pandas as pd
import os
from appli.scripts.features.features_utils import load_config, print_status

def compute_part_jeunes() -> pd.DataFrame:
    try:
        config = load_config()  # Load project configuration
        departement = config["departement"]
        maillage = int(config["maillage"])
        path = f"appli/data/features/recens_agrege_{maillage}m.csv"

        # Check if the aggregated census file exists
        if not os.path.exists(path):
            print_status("Aggregated census file missing", "err", path)
            return pd.DataFrame(columns=["idINSPIRE", "part_jeunes"])

        df = pd.read_csv(path)

        # Verify required columns
        required = ["idINSPIRE", "ind", "ind_0_3", "ind_4_5", "ind_6_10", "ind_11_17", "ind_18_24"]
        missing = [col for col in required if col not in df.columns]
        if missing:
            print_status(f"Missing columns in census file: {missing}", "err")
            return pd.DataFrame(columns=["idINSPIRE", "part_jeunes"])

        # Convert columns to numeric and handle NaN values
        for col in required[1:]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Calculate the population of young people and their proportion
        df["population_jeunes"] = (
            df["ind_0_3"] + df["ind_4_5"] + df["ind_6_10"] + df["ind_11_17"] + df["ind_18_24"]
        )
        df["part_jeunes"] = df.apply(
            lambda row: row["population_jeunes"] / row["ind"] if row["ind"] > 0 else 0, axis=1
        )

        return df[["idINSPIRE", "part_jeunes"]]

    except Exception as e:
        # Handle errors and return an empty DataFrame
        print_status("Error computing proportion of young people", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "part_jeunes"])

if __name__ == "__main__":
    config = load_config()  # Load project configuration
    maillage = config["maillage"]
    print_status("Computing proportion of young people", "info")
    result = compute_part_jeunes()
    output_path = f"appli/output/features/part_jeunes_{maillage}m.csv"
    result.to_csv(output_path, index=False)
    print_status("Proportion of young people exported", "ok", output_path)
