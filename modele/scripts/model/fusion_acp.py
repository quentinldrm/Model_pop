"""
Script : fusion_acp.py
Objective : Merge all feature CSV files present in output/features/
            on the common key 'idINSPIRE' for PCA analysis.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Data preparation step for factorial analysis (PCA).
"""

import pandas as pd
import glob
import os
from modele.scripts.features.features_utils import print_status  # facultatif si tu veux journaliser

# === PARAMETERS ===
FEATURES_DIR = "modele/output/features"
OUTPUT_PATH = "modele/output/fusion/variables_merged.csv"
EXTENSION = "*.csv"

# List all feature files
liste_fichiers = glob.glob(os.path.join(FEATURES_DIR, EXTENSION))

# Initialize the merged table
df_merged = None

if not liste_fichiers:
    print_status("No feature file found", "err")
    exit()

for fichier in liste_fichiers:
    df = pd.read_csv(fichier)

    if 'idINSPIRE' not in df.columns:
        raise ValueError(f"The file {fichier} does not contain the column 'idINSPIRE'")

    # Aggregate potential duplicates (e.g., merging POI + buildings)
    if df['idINSPIRE'].duplicated().any():
        df = df.groupby('idINSPIRE').mean(numeric_only=True).reset_index()

    # Progressive merge
    df_merged = df if df_merged is None else pd.merge(df_merged, df, on="idINSPIRE", how="outer")

# Replace missing values
if df_merged is not None:
    df_merged = df_merged.fillna(0)
else:
    raise ValueError("No merged data found to apply fillna.")

# Final export
os.makedirs("output/fusion", exist_ok=True)
df_merged.to_csv(OUTPUT_PATH, index=False)
print_status("PCA merge completed", "ok", f"File exported: {OUTPUT_PATH}")
