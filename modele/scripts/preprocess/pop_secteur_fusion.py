"""
Script : preprocess_pop_mobiliscope.py
Objective : Extract, aggregate, and merge Mobiliscope population data
            (day and night) for all available cities, producing a national GeoParquet file for day and night.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Complete preprocessing script for Mobiliscope data.
"""

import os
from pathlib import Path
import pandas as pd
import geopandas as gpd

# === GLOBAL PARAMETERS ===
DATA_DIR = Path("modele/data/mobiliscope")
TARGET_DIR = Path("modele/data/target")
TARGET_DIR.mkdir(exist_ok=True)
HEURES_JOUR = ["10am", "11am", "12am", "1pm", "2pm", "3pm", "4pm"]
HEURES_NUIT = ["12am", "1am", "2am", "3am", "4am", "5am", "6am"]


# Step 1: Calculate day and night averages from raw CSV files
def extraire_moyennes_population():
    print("[→] Extracting population averages (day/night) by city")
    for csv_path in DATA_DIR.glob("*_pop_choro_stacked.csv"):
        ville = csv_path.stem.split("_")[0]
        df = pd.read_csv(csv_path)

        df_jour = df[df["hour"].isin(HEURES_JOUR)].groupby("district")["pop0"].mean().reset_index()
        df_jour.to_csv(DATA_DIR / f"{ville}_pop_jour.csv", index=False)

        df_nuit = df[df["hour"].isin(HEURES_NUIT)].groupby("district")["pop0"].mean().reset_index()
        df_nuit.to_csv(DATA_DIR / f"{ville}_pop_nuit.csv", index=False)

        print(f"[✓] Averages exported for {ville}")


# Step 2: Join averages with sector files for each city
def associer_pop_aux_secteurs():
    print("[→] Associating averages with sectors by city")

    for csv_pop in DATA_DIR.glob("*_pop_jour.csv"):
        ville = csv_pop.stem.replace("_pop_jour", "")
        csv_nuit = DATA_DIR / f"{ville}_pop_nuit.csv"
        secteurs_path = DATA_DIR / f"{ville}_secteurs.parquet"

        if not (csv_nuit.exists() and secteurs_path.exists()):
            print(f"[!] Missing data for {ville}, skipped.")
            continue

        # Load data
        gdf = gpd.read_parquet(secteurs_path)
        df_jour = pd.read_csv(csv_pop)
        df_nuit = pd.read_csv(csv_nuit)

        # Harmonize sector codes
        df_jour["district"] = df_jour["district"].astype(str).str.zfill(3)
        df_nuit["district"] = df_nuit["district"].astype(str).str.zfill(3)
        gdf["CODE_SEC"] = gdf["CODE_SEC"].astype(str).str.zfill(3)

        # Attribute join
        gdf_jour = gdf.merge(df_jour, left_on="CODE_SEC", right_on="district", how="left")
        gdf_nuit = gdf.merge(df_nuit, left_on="CODE_SEC", right_on="district", how="left")

        # Export
        gdf_jour.to_parquet(DATA_DIR / f"{ville}_pop_jour_secteur.parquet", compression="brotli", index=False)
        gdf_nuit.to_parquet(DATA_DIR / f"{ville}_pop_nuit_secteur.parquet", compression="brotli", index=False)

        print(f"[✓] Joins completed for {ville}")


# Step 3: Merge cities into a single national file
def fusion_finale_par_type():
    print("[→] National merge of cities (day and night)")

    def merge_parquets(pattern, output_name):
        gdfs = []
        for f in DATA_DIR.glob(pattern):
            gdf = gpd.read_parquet(f)
            ville = f.stem.split("_pop_")[0]
            gdf["ville"] = ville
            gdfs.append(gdf)

        if not gdfs:
            print(f"[!] No files matching {pattern}")
            return

        merged = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
        merged["secteur_uid"] = merged["ville"] + "_" + merged["CODE_SEC"].astype(str)
        merged.to_parquet(TARGET_DIR / output_name, compression="brotli", index=False)

        print(f"[✓] Merged file: {output_name}")

    merge_parquets("*_pop_jour_secteur.parquet", "all_villes_pop_jour_secteurs.parquet")
    merge_parquets("*_pop_nuit_secteur.parquet", "all_villes_pop_nuit_secteurs.parquet")


# Sequential execution of the pipeline
def main():
    print("[INFO] Mobiliscope pipeline started")
    extraire_moyennes_population()
    associer_pop_aux_secteurs()
    fusion_finale_par_type()
    print("[INFO] Mobiliscope pipeline successfully completed")


if __name__ == "__main__":
    main()
    main()
