"""
Script : features_pipeline.py
Objective : Execute the pipeline for generating spatial features based on the grid and other input data.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Master script for feature generation in the modeling pipeline.
"""

import geopandas as gpd
from appli.scripts.features.features_utils import load_config, print_status

from appli.scripts.features.feature_poi import compute_score_poi_pondere
from appli.scripts.features.feature_emp_est import compute_emplois_estimes_pondere
from appli.scripts.features.feature_dens_eta import compute_densite_etablissements
from appli.scripts.features.feature_dens_com import compute_densite_commerces
from appli.scripts.features.feature_in_mix_fonc import compute_indice_mixite_fonctionnelle
from appli.scripts.features.feature_pop_act import compute_part_population_active
from appli.scripts.features.feature_jeune import compute_part_jeunes
from appli.scripts.features.feature_shape_index import compute_shape_index_moyen
from appli.scripts.features.feature_h_pond import compute_hauteur_ponderee_surface
from appli.scripts.features.feature_sd_h import compute_ecart_type_hauteur
from appli.scripts.features.feature_sd_area import compute_ecart_type_surface_batiment
from appli.scripts.features.feature_d_moy_bati import compute_distance_moyenne_batiments

def safe_run(name, func, grid):
    try:
        print_status(f"Starting {name}", "info")
        df = func(grid) if grid is not None else func()
        df.to_csv(f"appli/output/features/{name}_{config['maillage']}m.csv", index=False)
        print_status(f"{name} completed", "ok")
    except Exception as e:
        print_status(f"{name} failed", "err", str(e))

def main():
    global config
    config = load_config()  # Load project configuration
    departement = config["departement"]
    maillage = config["maillage"]

    # Load the spatial grid
    grid = gpd.read_file(f"appli/output/grid/grid_{departement}_{maillage}m.geojson").to_crs("EPSG:2154")

    print("=== STARTING FEATURE PIPELINE ===")

    # Execute feature generation functions
    safe_run("score_poi_pondere", compute_score_poi_pondere, grid)
    safe_run("emplois_estimes_pondere", compute_emplois_estimes_pondere, grid)
    safe_run("densite_etablissements", compute_densite_etablissements, grid)
    safe_run("densite_commerces", compute_densite_commerces, grid)
    safe_run("indice_mixite_fonctionnelle", compute_indice_mixite_fonctionnelle, grid)
    safe_run("part_population_active", compute_part_population_active, None)
    safe_run("part_jeunes", compute_part_jeunes, None)
    safe_run("shape_index_moyen", compute_shape_index_moyen, grid)
    safe_run("hauteur_ponderee_surface", compute_hauteur_ponderee_surface, grid)
    safe_run("ecart_type_hauteur", compute_ecart_type_hauteur, grid)
    safe_run("ecart_type_surface_batiment", compute_ecart_type_surface_batiment, grid)
    safe_run("distance_moyenne_batiments", compute_distance_moyenne_batiments, grid)

    print("=== FEATURE PIPELINE COMPLETED ===")

if __name__ == "__main__":
    main()
