"""
Script : in_mix_fonc.py
Objective : Compute the functional mix index (Shannon entropy)
            based on the grouping of NAF codes into major urban functions.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Feature generation step in the features pipeline.
"""

import os
import pandas as pd
import geopandas as gpd
import numpy as np
from modele.scripts.features.features_utils import print_status

# === PATHS ===
SIRENE_PATH = "modele/data/raw/sirene.parquet"
GRID_PATH = "modele/output/grid/grid_mobiliscope_200m.parquet"
OUTPUT_PATH = "modele/output/features/indice_mixite_fonctionnelle_200m.csv"

# === Dictionary for grouping NAF2 → major urban functions ===
NAF2_TO_FONCTION = {
    "45": "automobile", "46": "commerce_gros", "47": "commerce_detail",
    "55": "hebergement", "56": "restauration", "58": "edition", "59": "audiovisuel",
    "60": "radio_tv", "61": "telecom", "62": "informatique", "63": "services_info",
    "64": "banque", "65": "assurance", "66": "finance", "68": "immobilier",
    "69": "juridique", "70": "siège_social", "71": "architecture", "72": "recherche",
    "73": "publicite", "74": "design", "75": "veterinaire", "77": "location",
    "78": "interim", "79": "voyage", "80": "securite", "81": "services_generaux",
    "82": "divers_bureau", "85": "enseignement", "86": "sante", "87": "hebergement_sante",
    "88": "action_sociale", "90": "arts", "91": "associations", "92": "loisirs",
    "93": "sport", "94": "organisation", "95": "reparation_biens", "96": "autres_services"
}


# Main function
def compute_indice_mixite_fonctionnelle(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        if not os.path.exists(SIRENE_PATH):
            print_status("SIRENE file missing", "err", SIRENE_PATH)
            return pd.DataFrame(columns=["idINSPIRE", "indice_mixite_fonctionnelle"])

        # Load SIRENE
        gdf = gpd.read_parquet(SIRENE_PATH).to_crs("EPSG:2154")
        gdf["x"] = pd.to_numeric(gdf["longitude"], errors="coerce")
        gdf["y"] = pd.to_numeric(gdf["latitude"], errors="coerce")
        gdf = gdf.dropna(subset=["x", "y"])
        gdf["geometry"] = gpd.points_from_xy(gdf["x"], gdf["y"])
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs="EPSG:2154")

        # Map NAF2 codes to major functions
        gdf["naf2"] = gdf["activitePrincipaleEtablissement"].astype(str).str[:2]
        gdf["fonction"] = gdf["naf2"].map(NAF2_TO_FONCTION).fillna("other")

        # Clean unnecessary columns
        gdf.drop(columns=["index_right"], errors="ignore", inplace=True)
        grid.drop(columns=["index_right"], errors="ignore", inplace=True)

        # Spatial join
        print_status("Spatial join SIRENE → grid", "info")
        joined = gpd.sjoin(grid, gdf[["geometry", "fonction"]], how="left", predicate="contains")

        # Compute Shannon entropy per grid cell
        print_status("Computing Shannon entropy", "info")
        def entropy(group):
            counts = group["fonction"].value_counts()
            probs = counts / counts.sum()
            return -(probs * np.log(probs)).sum()

        entropies = joined.groupby("idINSPIRE", group_keys=False).apply(entropy).reset_index(name="indice_mixite_fonctionnelle")
        return entropies

    except Exception as e:
        print_status("Error computing indice_mixite_fonctionnelle", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "indice_mixite_fonctionnelle"])


# Direct execution
if __name__ == "__main__":
    print_status("Computing indice_mixite_fonctionnelle", "info")
    grid = gpd.read_parquet(GRID_PATH).to_crs("EPSG:2154")
    result = compute_indice_mixite_fonctionnelle(grid)
    result.to_csv(OUTPUT_PATH, index=False)
    print_status("Functional mix index exported", "ok", OUTPUT_PATH)
