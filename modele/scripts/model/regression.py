""" 
Script : regression.py
Objective : Perform bivariate and multiple linear regressions + residual maps,
            absolute errors, export predictions + average maps per city.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Spatial performance analysis by sector.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
import statsmodels.api as sm
from shapely.ops import unary_union
from shapely.geometry import Polygon, MultiPolygon
from sklearn.metrics import mean_squared_error, r2_score
from math import sqrt
from modele.scripts.features.features_utils import print_status

# === PATHS ===
FUSION_PATH = "modele/output/fusion/features_modele.csv"
SECTEURS_PATH = "modele/data/processed/secteurs.parquet"
FIG_DIR = "modele/output/regression/figures"
STATS_DIR = "modele/output/regression"
PREDICTION_DIR = "modele/output/regression/predictions"
EXPORT_DIR = "modele/output/regression/export"
os.makedirs(FIG_DIR, exist_ok=True)
os.makedirs(STATS_DIR, exist_ok=True)
os.makedirs(PREDICTION_DIR, exist_ok=True)
os.makedirs(EXPORT_DIR, exist_ok=True)

STATS_CSV = os.path.join(STATS_DIR, "regression_stats.csv")
SUMMARY_JOUR = os.path.join(STATS_DIR, "regression_day_summary.txt")
SUMMARY_NUIT = os.path.join(STATS_DIR, "regression_night_summary.txt")


def clean_nom(s):
    return s.str.lower().str.strip().str.replace(r"[^\w]+", "_", regex=True)

def remove_holes(geom):
    if geom is None or geom.is_empty:
        return geom
    if isinstance(geom, Polygon):
        return Polygon(geom.exterior)
    elif isinstance(geom, MultiPolygon):
        return MultiPolygon([Polygon(p.exterior) for p in geom.geoms])
    else:
        return geom

def carte_residus(df_pred, cible):
    df_pred["secteur_uid"] = df_pred["secteur_uid"].str.lower()
    moyennes = df_pred.groupby("secteur_uid", as_index=False)[["residu", "abs_residu"]].mean()

    gdf = gpd.read_parquet(SECTEURS_PATH).to_crs("EPSG:2154")
    gdf["ENQUETE"] = clean_nom(gdf["ENQUETE"])
    gdf["CODE_SEC"] = clean_nom(gdf["CODE_SEC"].astype(str))
    gdf["secteur_uid"] = gdf["ENQUETE"] + "_" + gdf["CODE_SEC"]
    gdf = gdf.merge(moyennes, on="secteur_uid", how="inner")

    gdf["geometry"] = gdf["geometry"].buffer(0)
    geometries = gdf.groupby("ENQUETE")["geometry"].apply(lambda x: unary_union(x.tolist()))
    moyennes = gdf.groupby("ENQUETE")[["residu", "abs_residu"]].mean()

    gdf_villes = gpd.GeoDataFrame(
        moyennes,
        geometry=geometries,
        crs=gdf.crs
    ).reset_index()

    gdf_villes = gdf_villes[~gdf_villes["geometry"].is_empty & gdf_villes["geometry"].is_valid]
    gdf_villes["geometry"] = gdf_villes["geometry"].apply(lambda g: remove_holes(g.buffer(0)))

    gdf_villes.to_parquet(os.path.join(EXPORT_DIR, f"city_residuals_regression_{cible}.parquet"), index=False)

    vmax = gdf_villes["residu"].abs().max()
    vmin = -vmax

    fig, ax = plt.subplots(figsize=(12, 10))
    gdf_villes.plot(
        column="residu",
        cmap="coolwarm",
        legend=True,
        edgecolor="grey",
        linewidth=0.3,
        ax=ax,
        legend_kwds={"label": "Average residual per city", "shrink": 0.7},
        vmin=vmin, vmax=vmax
    )
    ax.set_title(f"Average error per city - Regression - ({cible})", fontsize=16)
    ax.axis("off")
    plt.tight_layout()
    plt.savefig(f"{FIG_DIR}/average_city_residuals_{cible}.svg", dpi=600)
    plt.close()

    fig, ax = plt.subplots(figsize=(12, 10))
    gdf_villes.plot(
        column="abs_residu",
        cmap="OrRd",
        legend=True,
        edgecolor="grey",
        linewidth=0.3,
        ax=ax,
        legend_kwds={"label": "Average absolute residual per city", "shrink": 0.7}
    )
    ax.set_title(f"Average absolute error per city - Regression - ({cible})", fontsize=16)
    ax.axis("off")
    plt.tight_layout()
    plt.savefig(f"{FIG_DIR}/average_absolute_city_residuals_{cible}.svg", dpi=600)
    plt.close()

def carte_residus_idf(df_pred, cible):
    df_pred["secteur_uid"] = df_pred["secteur_uid"].str.lower()

    gdf = gpd.read_parquet(SECTEURS_PATH).to_crs("EPSG:2154")
    gdf["ENQUETE"] = gdf["ENQUETE"].str.lower()
    gdf["CODE_SEC"] = gdf["CODE_SEC"].astype(str)
    gdf["secteur_uid"] = gdf["ENQUETE"] + "_" + gdf["CODE_SEC"]
    merged = gdf.merge(df_pred, on="secteur_uid", how="left")
    gdf_idf = merged[merged["secteur_uid"].str.startswith("idf_")]

    vmax = gdf_idf["residu"].abs().max()
    vmin = -vmax

    fig, ax = plt.subplots(figsize=(12, 10))
    gdf_idf.plot(
        column="residu", cmap="coolwarm", legend=True,
        edgecolor="black", linewidth=0.2, ax=ax,
        legend_kwds={"label": "Résidu (réel - prédit)", "shrink": 0.7},
        vmin=vmin, vmax=vmax
    )
    ax.set_title(f"Carte détaillée des résidus - IDF - Régression - ({cible})", fontsize=16)
    ax.axis("off")
    plt.tight_layout()
    plt.savefig(f"{FIG_DIR}/residus_idf_{cible}.svg", dpi=600)
    plt.close()

    fig, ax = plt.subplots(figsize=(12, 10))
    gdf_idf.plot(
        column="abs_residu", cmap="OrRd", legend=True,
        edgecolor="black", linewidth=0.2, ax=ax,
        legend_kwds={"label": "Résidu absolu |réel - prédit|", "shrink": 0.7}
    )
    ax.set_title(f"Carte détaillée des résidus absolus - IDF - Régression - ({cible})", fontsize=16)
    ax.axis("off")
    plt.tight_layout()
    plt.savefig(f"{FIG_DIR}/residus_abs_idf_{cible}.svg", dpi=600)
    plt.close()

def carte_residus_lyon(df_pred, cible):
    df_pred["secteur_uid"] = df_pred["secteur_uid"].str.lower()

    gdf = gpd.read_parquet(SECTEURS_PATH).to_crs("EPSG:2154")
    gdf["ENQUETE"] = gdf["ENQUETE"].str.lower()
    gdf["CODE_SEC"] = gdf["CODE_SEC"].astype(str)
    gdf["secteur_uid"] = gdf["ENQUETE"] + "_" + gdf["CODE_SEC"]
    merged = gdf.merge(df_pred, on="secteur_uid", how="left")
    gdf_lyon = merged[merged["secteur_uid"].str.startswith("lyon_")]

    vmax = gdf_lyon["residu"].abs().max()
    vmin = -vmax

    fig, ax = plt.subplots(figsize=(12, 10))
    gdf_lyon.plot(
        column="residu", cmap="coolwarm", legend=True,
        edgecolor="black", linewidth=0.2, ax=ax,
        legend_kwds={"label": "Résidu (réel - prédit)", "shrink": 0.7},
        vmin=vmin, vmax=vmax
    )
    ax.set_title(f"Carte détaillée des résidus - Lyon - Régression - ({cible})", fontsize=16)
    ax.axis("off")
    plt.tight_layout()
    plt.savefig(f"{FIG_DIR}/residus_lyon_{cible}.svg", dpi=600)
    plt.close()

    fig, ax = plt.subplots(figsize=(12, 10))
    gdf_lyon.plot(
        column="abs_residu", cmap="OrRd", legend=True,
        edgecolor="black", linewidth=0.2, ax=ax,
        legend_kwds={"label": "Résidu absolu |réel - prédit|", "shrink": 0.7}
    )
    ax.set_title(f"Carte détaillée des résidus absolus - Lyon - Régression - ({cible})", fontsize=16)
    ax.axis("off")
    plt.tight_layout()
    plt.savefig(f"{FIG_DIR}/residus_abs_lyon_{cible}.svg", dpi=600)
    plt.close()

def analyser_regressions():
    print_status("Loading merged data", "info")
    df = pd.read_csv(FUSION_PATH)

    X_cols = [col for col in df.columns if col not in ["secteur_uid", "population_jour", "population_nuit"]]
    X = df[X_cols]
    y_jour = df["population_jour"]
    y_nuit = df["population_nuit"]

    print_status("Performing multiple linear regressions", "info")
    X_const = sm.add_constant(X)
    model_jour = sm.OLS(y_jour, X_const).fit()
    model_nuit = sm.OLS(y_nuit, X_const).fit()

    y_pred_jour = model_jour.predict(X_const)
    y_pred_nuit = model_nuit.predict(X_const)

    rmse_jour = sqrt(mean_squared_error(y_jour, y_pred_jour))
    rmse_nuit = sqrt(mean_squared_error(y_nuit, y_pred_nuit))

    stats_df = pd.DataFrame([
        {"modele": "regression", "cible": "pop_jour", "r2": model_jour.rsquared, "rmse": rmse_jour},
        {"modele": "regression", "cible": "pop_nuit", "r2": model_nuit.rsquared, "rmse": rmse_nuit}
    ])
    stats_df.to_csv(STATS_CSV, index=False)

    with open(SUMMARY_JOUR, "w") as f:
        f.write(model_jour.summary().as_text())
    with open(SUMMARY_NUIT, "w") as f:
        f.write(model_nuit.summary().as_text())

    for cible, y, y_pred in [("pop_jour", y_jour, y_pred_jour), ("pop_nuit", y_nuit, y_pred_nuit)]:
        df_pred = pd.DataFrame({
            "secteur_uid": df["secteur_uid"],
            "reel": y,
            "pred": y_pred,
            "residu": y - y_pred,
            "abs_residu": (y - y_pred).abs()
        })
        out_path = os.path.join(PREDICTION_DIR, f"predictions_{cible}.parquet")
        df_pred.to_parquet(out_path, index=False)
        carte_residus(df_pred, cible)
        carte_residus_idf(df_pred, cible)
        carte_residus_lyon(df_pred, cible)

    print_status("Regression + residual maps completed", "ok")

if __name__ == "__main__":
    analyser_regressions()
