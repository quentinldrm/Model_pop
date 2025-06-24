"""
Script : feature_pop_act.py
Objective : Compute the proportion of active population within each grid cell based on aggregated census data.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for generating spatial features related to active population density.
"""

import pandas as pd
import os
from appli.scripts.features.features_utils import load_config, print_status

def compute_part_population_active() -> pd.DataFrame:
    try:
        config = load_config()  # Load project configuration
        departement = config["departement"]
        maillage = int(config["maillage"])
        path = f"appli/data/features/recens_agrege_{maillage}m.csv"

        # Check if the aggregated census file exists
        if not os.path.exists(path):
            print_status("Aggregated census file missing", "err", path)
            return pd.DataFrame(columns=["idINSPIRE", "part_population_active"])

        df = pd.read_csv(path)

        # Verify required columns
        required = ["idINSPIRE", "ind", "ind_18_24", "ind_25_39", "ind_40_54", "ind_55_64"]
        missing = [col for col in required if col not in df.columns]
        if missing:
            print_status(f"Missing columns in census file: {missing}", "err")
            return pd.DataFrame(columns=["idINSPIRE", "part_population_active"])

        # Convert columns to numeric and handle NaN values
        for col in required[1:]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Calculate active population
        df["population_active"] = df["ind_18_24"] + df["ind_25_39"] + df["ind_40_54"] + df["ind_55_64"]
        df["part_population_active"] = df.apply(
            lambda row: row["population_active"] / row["ind"] if row["ind"] > 0 else 0, axis=1
        )

        return df[["idINSPIRE", "part_population_active"]]

    except Exception as e:
        # Handle errors and return an empty DataFrame
        print_status("Error computing proportion of active population", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "part_population_active"])

if __name__ == "__main__":
    config = load_config()  # Load project configuration
    maillage = config["maillage"]
    print_status("Computing proportion of active population", "info")
    result = compute_part_population_active()
    output_path = f"appli/output/features/part_population_active_{maillage}m.csv"
    result.to_csv(output_path, index=False)
    print_status("Proportion of active population exported", "ok", output_path)
