"""
Script : features_pipeline.py
Objective : Sequentially execute the generation of all spatial features
            for each grid cell (POI score, jobs, densities, morphology, socio-demographics, etc.).
Author : LEDERMANN Quentin
Date : June 2025
Usage : Master pipeline of the 'features' module, to be run after preprocessing.
"""

import geopandas as gpd
from scripts.features.features_utils import load_config, print_status

# === IMPORT DES FONCTIONS DE FEATURES ===
from modele.scripts.features.score_poi import compute_score_poi_pondere
from modele.scripts.features.emp_est import compute_emplois_estimes_pondere
from modele.scripts.features.dens_eta import compute_densite_etablissements
from modele.scripts.features.dens_com import compute_densite_commerces
from modele.scripts.features.in_mix_fonc import compute_indice_mixite_fonctionnelle
from modele.scripts.features.part_pop_act import compute_part_population_active
from modele.scripts.features.part_jeune import compute_part_jeunes
from modele.scripts.features.shape_index import compute_shape_index_moyen
from modele.scripts.features.h_pond import compute_hauteur_ponderee_surface
from modele.scripts.features.sd_h import compute_ecart_type_hauteur
from modele.scripts.features.sd_area import compute_ecart_type_surface_batiment
from modele.scripts.features.dis_moy_bati import compute_distance_moyenne_batiments
from modele.scripts.features.larg_moy_voirie import compute_largeur_moyenne_voirie
from modele.scripts.features.dens_voirie import compute_densite_voirie_optimisee
from modele.scripts.features.vol_moy import compute_volume_moyen_par_maille


# Executes a feature function and saves its result as a CSV
def safe_run(name, func, *args):
    try:
        print_status(f"Starting: {name}", "info")
        df = func(*args)
        df.to_csv(f"output/features/{name}_{config['maillage']}m.csv", index=False)
        print_status(f"{name} completed", "ok")
    except Exception as e:
        print_status(f"{name} failed", "err", str(e))


# Main pipeline
def main():
    global config
    config = load_config()
    departement = config["departement"]
    maillage = config["maillage"]

    print_status("=== STARTING FEATURES PIPELINE ===", "info")

    # Load the grid
    grid = gpd.read_file(f"output/grid/grid_{departement}_{maillage}m.geojson").to_crs("EPSG:2154")

    # === Execute features ===
    safe_run("score_poi_pondere", compute_score_poi_pondere, grid)
    safe_run("emplois_estimes_pondere", compute_emplois_estimes_pondere, grid)
    safe_run("densite_etablissements", compute_densite_etablissements, grid)
    safe_run("densite_commerces", compute_densite_commerces)
    safe_run("indice_mixite_fonctionnelle", compute_indice_mixite_fonctionnelle, grid)
    safe_run("part_population_active", compute_part_population_active, grid, gpd.read_parquet("modele/data/raw/recens.parquet"))
    safe_run("part_jeunes", compute_part_jeunes, grid, gpd.read_parquet("modele/data/raw/recens.parquet"))
    safe_run("shape_index_moyen", compute_shape_index_moyen, grid)
    safe_run("hauteur_ponderee_surface", compute_hauteur_ponderee_surface, grid)
    safe_run("ecart_type_hauteur", compute_ecart_type_hauteur, grid)
    safe_run("ecart_type_surface_batiment", compute_ecart_type_surface_batiment, grid)
    safe_run("distance_moyenne_batiments", compute_distance_moyenne_batiments, grid, gpd.read_parquet("modele/data/processed/BATIMENT.parquet"))
    safe_run("largeur_moyenne_voirie", compute_largeur_moyenne_voirie, grid)
    safe_run("densite_voirie", compute_densite_voirie_optimisee)
    safe_run("volume_moyen_bati", compute_volume_moyen_par_maille, grid)

    print_status("=== FEATURES PIPELINE COMPLETED ===", "ok")


if __name__ == "__main__":
    main()
