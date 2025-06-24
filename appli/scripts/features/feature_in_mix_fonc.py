"""
Script : feature_in_mix_fonc.py
Objective : Compute the functional mix index (Shannon entropy) within each grid cell,
            based on SIRENE data and urban functions grouped by NAF codes.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for generating spatial features related to functional diversity.
"""

import geopandas as gpd
import pandas as pd
import numpy as np
import os
from appli.scripts.features.features_utils import load_config, print_status

# Group NAF level 2 codes into major urban functions
NAF2_TO_FONCTION = {
    "45": "automobile",
    "46": "commerce_gros",
    "47": "commerce_detail",
    "55": "hebergement",
    "56": "restauration",
    "58": "edition",
    "59": "audiovisuel",
    "60": "radio_tv",
    "61": "telecom",
    "62": "informatique",
    "63": "services_info",
    "64": "banque",
    "65": "assurance",
    "66": "finance",
    "68": "immobilier",
    "69": "juridique",
    "70": "siège_social",
    "71": "architecture",
    "72": "recherche",
    "73": "publicite",
    "74": "design",
    "75": "veterinaire",
    "77": "location",
    "78": "interim",
    "79": "voyage",
    "80": "securite",
    "81": "services_generaux",
    "82": "divers_bureau",
    "85": "enseignement",
    "86": "sante",
    "87": "hebergement_sante",
    "88": "action_sociale",
    "90": "arts",
    "91": "associations",
    "92": "loisirs",
    "93": "sport",
    "94": "organisation",
    "95": "reparation_biens",
    "96": "autres_services",
}

def compute_indice_mixite_fonctionnelle(grid: gpd.GeoDataFrame) -> pd.DataFrame:
    try:
        config = load_config()  # Load project configuration
        departement = config["departement"]
        path_sirene = f"appli/data/sirene/sirene.geojson"

        # Check if the SIRENE file exists
        if not os.path.exists(path_sirene):
            print_status("SIRENE file missing", "err", path_sirene)
            return pd.DataFrame(columns=["idINSPIRE", "indice_mixite_fonctionnelle"])

        # Load SIRENE data and reconstruct geometry
        gdf = gpd.read_file(path_sirene)
        gdf["x"] = pd.to_numeric(gdf["longitude"], errors="coerce")
        gdf["y"] = pd.to_numeric(gdf["latitude"], errors="coerce")
        gdf = gdf.dropna(subset=["x", "y"])
        gdf["geometry"] = gpd.points_from_xy(gdf["x"], gdf["y"])
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs="EPSG:2154")

        # Group activities into urban functions
        gdf["naf2"] = gdf["activitePrincipaleEtablissement"].astype(str).str[:2]
        gdf["fonction"] = gdf["naf2"].map(NAF2_TO_FONCTION).fillna("autre")

        # Spatial join: assign establishments to grid cells
        joined = gpd.sjoin(grid, gdf[["geometry", "fonction"]], how="left", predicate="contains")

        # Compute Shannon entropy for each grid cell
        def entropy(group):
            counts = group["fonction"].value_counts()
            probs = counts / counts.sum()
            return -(probs * np.log(probs)).sum()

        entropies = joined.groupby("idINSPIRE", group_keys=False).apply(entropy).reset_index(name="indice_mixite_fonctionnelle")

        return entropies

    except Exception as e:
        # Handle errors and return an empty DataFrame
        print_status("Error computing functional mix index", "err", str(e))
        return pd.DataFrame(columns=["idINSPIRE", "indice_mixite_fonctionnelle"])

if __name__ == "__main__":
    config = load_config()  # Load project configuration
    maillage = config["maillage"]
    departement = config["departement"]
    grid_path = f"appli/output/grid/grid_{departement}_{maillage}m.geojson"
    grid = gpd.read_file(grid_path).to_crs("EPSG:2154")

    print_status("Computing functional mix index", "info")
    result = compute_indice_mixite_fonctionnelle(grid)
    output_path = f"appli/output/features/indice_mixite_fonctionnelle_{maillage}m.csv"
    result.to_csv(output_path, index=False)
    print_status("Functional mix index exported", "ok", output_path)
