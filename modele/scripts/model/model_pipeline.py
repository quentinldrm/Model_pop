""" 
Script : model_pipeline.py
Objective : Orchestrate the complete execution of the modeling module:
            data merging, linear regressions, Random Forest, XGBoost, PCA.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Final pipeline of the model module.
"""

from modele.scripts.model.fusion_modele import fusionner_features_par_secteur, ajouter_cibles_populations
from modele.scripts.model.regression import analyser_regressions
from modele.scripts.model.random_forest import analyse_random_forest
from modele.scripts.model.xg_boost import analyse_xgboost


import pandas as pd
import os
from modele.scripts.features.features_utils import print_status


def run_model_pipeline():
    print_status("=== STARTING MODELING PIPELINE ===", "info")

    # === 1. Merge
    print_status("Merging features + targets", "info")
    df = fusionner_features_par_secteur()
    df = ajouter_cibles_populations(df)
    os.makedirs("modele/output/fusion", exist_ok=True)
    df.to_parquet("modele/output/fusion/features_population.parquet", index=False)


    # === 3. Models
    analyser_regressions()
    analyse_random_forest()
    analyse_xgboost()

    print_status("=== MODELING PIPELINE COMPLETED ===", "ok")


if __name__ == "__main__":
    run_model_pipeline()
