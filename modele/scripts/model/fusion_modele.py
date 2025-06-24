""" 
Script : fusion_modele.py (corrected)
Objective : Merge all features by Mobiliscope sector (weighted by surface area)
            and add Mobiliscope population targets (day/night).
Author : LEDERMANN Quentin
Date : June 2025
Usage : Data preparation for modeling.
"""

import pandas as pd
import geopandas as gpd
import os
from modele.scripts.features.features_utils import print_status

# === PATHS ===
FEATURES_PATH = "modele/output/features"
SURFACES_PATH = "modele/data/processed/surf_bati_200m.csv"
SECTEURS_PATH = "modele/data/processed/secteurs.parquet"
CIBLE_JOUR_PATH = "modele/data/target/all_villes_pop_jour_secteurs.geojson"
CIBLE_NUIT_PATH = "modele/data/target/all_villes_pop_nuit_secteurs.geojson"
OUTPUT_PATH = "modele/output/fusion/features_modele.csv"

# Cleaning function
def clean_nom(s):
    return s.str.lower().str.strip().str.replace(r"[^\w]+", "_", regex=True)

# Function 1: merge 200m features into Mobiliscope sectors
def fusionner_features_par_secteur():
    print_status("Merging features by sector", "info")

    surfaces = pd.read_csv(SURFACES_PATH)
    surfaces["idINSPIRE"] = surfaces["idINSPIRE"].astype(str)

    secteurs = gpd.read_parquet(SECTEURS_PATH).to_crs("EPSG:2154")
    secteurs["secteur_uid"] = clean_nom(secteurs["ENQUETE"]) + "_" + clean_nom(secteurs["CODE_SEC"])

    # Explicit removal of the index_right column if it exists in secteurs
    if "index_right" in secteurs.columns:
        secteurs = secteurs.drop(columns=["index_right"])

    # Grid → sectors join
    surf_gdf = gpd.read_parquet("modele/output/grid/grid_mobiliscope_200m.parquet").to_crs("EPSG:2154")
    surf_gdf["idINSPIRE"] = surf_gdf["idINSPIRE"].astype(str)  # Explicit conversion to str
    surf_gdf = surf_gdf.merge(surfaces, on="idINSPIRE", how="left")

    # Explicit removal of the index_right column if it exists before the join
    if "index_right" in surf_gdf.columns:
        surf_gdf = surf_gdf.drop(columns=["index_right"])

    joint = gpd.sjoin(surf_gdf, secteurs[["secteur_uid", "geometry"]], how="inner", predicate="intersects")

    # Suppression explicite de la colonne index_right ajoutée automatiquement
    if "index_right" in joint.columns:
        joint = joint.drop(columns=["index_right"])

    # Fusion pondérée
    fichiers = [f for f in os.listdir(FEATURES_PATH) if f.endswith(".csv")]
    all_vars = []

    for fichier in fichiers:
        df = pd.read_csv(os.path.join(FEATURES_PATH, fichier))
        nom_var = fichier.replace("_200m.csv", "").replace(".csv", "")
        if "idINSPIRE" not in df.columns :
            continue
        # Conversion explicite de la colonne idINSPIRE en str dans les deux DataFrames
        df["idINSPIRE"] = df["idINSPIRE"].astype(str)
        joint["idINSPIRE"] = joint["idINSPIRE"].astype(str)

        # Fusion des DataFrames
        df = df.merge(joint[["idINSPIRE", "secteur_uid", "surf_batie"]], on="idINSPIRE", how="inner")
        df[nom_var] = pd.to_numeric(df.iloc[:, 1], errors="coerce")  # force conversion
        df["pond"] = df[nom_var] * df["surf_batie"]
        agg = df.groupby("secteur_uid").agg(somme_pond=("pond", "sum"), somme_surface=("surf_batie", "sum")).reset_index()
        agg[nom_var + "_secteur"] = agg["somme_pond"] / agg["somme_surface"]
        all_vars.append(agg[["secteur_uid", nom_var + "_secteur"]])

    # Fusion finale
    df_final = all_vars[0]
    for df in all_vars[1:]:
        df_final = df_final.merge(df, on="secteur_uid", how="outer")

    # Renommer les colonnes pour supprimer le suffixe "_secteur"
    df_final.rename(columns=lambda col: col.replace("_secteur", "") if "_secteur" in col else col, inplace=True)

    return df_final


# Fonction 2 : ajout des cibles Mobiliscope jour/nuit
def ajouter_cibles_populations(df_features):
    print_status("Adding Mobiliscope targets", "info")

    # Read GeoJSON files
    pop_jour = gpd.read_file(CIBLE_JOUR_PATH).rename(columns={"pop0": "population_jour"})
    pop_nuit = gpd.read_file(CIBLE_NUIT_PATH).rename(columns={"pop0": "population_nuit"})

    # Clean and prepare data
    pop_jour["secteur_uid"] = clean_nom(pop_jour["ENQUETE"]) + "_" + clean_nom(pop_jour["CODE_SEC"])
    pop_nuit["secteur_uid"] = clean_nom(pop_nuit["ENQUETE"]) + "_" + clean_nom(pop_nuit["CODE_SEC"])

    # Select necessary columns
    pop_jour = pop_jour[["secteur_uid", "population_jour"]]
    pop_nuit = pop_nuit[["secteur_uid", "population_nuit"]]

    # Merge day and night targets
    pop = pop_jour.merge(pop_nuit, on="secteur_uid", how="outer")

    # Merge with features
    fusion = df_features.merge(pop, on="secteur_uid", how="inner")

    return fusion


# Main
if __name__ == "__main__":
    print_status("Starting merge for modeling", "info")
    df = fusionner_features_par_secteur()
    df = ajouter_cibles_populations(df)
    df.to_csv(OUTPUT_PATH, index=False)
    print_status("Merge completed", "ok", OUTPUT_PATH)
