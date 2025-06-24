""" 
Script : xg_boost.py
Objective : Model population (day and night) using XGBoost + export predictions,
            residual maps, and variable importance.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Complementary non-linear modeling.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas as gpd
from shapely.ops import unary_union
from shapely.geometry import Polygon, MultiPolygon
from sklearn.metrics import r2_score, mean_squared_error, root_mean_squared_error
from xgboost import XGBRegressor
import os

# === PATHS ===
FUSION_PATH = "modele/output/fusion/features_modele.csv"
SECTEURS_PATH = "modele/data/processed/secteurs.parquet"
FIG_DIR = "modele/output/xgboost/figures"
STATS_DIR = "modele/output/xgboost"
PREDICTION_DIR = "modele/output/xgboost/predictions"
EXPORT_DIR = "modele/output/xgboost/export"
os.makedirs(FIG_DIR, exist_ok=True)
os.makedirs(STATS_DIR, exist_ok=True)
os.makedirs(PREDICTION_DIR, exist_ok=True)
os.makedirs(EXPORT_DIR, exist_ok=True)

plt.rcParams["font.family"] = "Arial"

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

    gdf_villes.to_parquet(os.path.join(EXPORT_DIR, f"city_residuals_xgb_{cible}.parquet"), index=False)

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
    ax.set_title(f"Average error per city - XGBoost - ({cible})", fontsize=16)
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
    ax.set_title(f"Average absolute error per city - XGBoost - ({cible})", fontsize=16)
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
        legend_kwds={"label": "Residual (actual - predicted)", "shrink": 0.7},
        vmin=vmin, vmax=vmax
    )
    ax.set_title(f"Detailed residual map - IDF - XGBoost - ({cible})", fontsize=16)
    ax.axis("off")
    plt.tight_layout()
    plt.savefig(f"{FIG_DIR}/residus_idf_{cible}.svg", dpi=600)
    plt.close()

    fig, ax = plt.subplots(figsize=(12, 10))
    gdf_idf.plot(
        column="abs_residu", cmap="OrRd", legend=True,
        edgecolor="black", linewidth=0.2, ax=ax,
        legend_kwds={"label": "Absolute residual |actual - predicted|", "shrink": 0.7}
    )
    ax.set_title(f"Detailed absolute residual map - IDF - XGBoost - ({cible})", fontsize=16)
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
        legend_kwds={"label": "Residual (actual - predicted)", "shrink": 0.7},
        vmin=vmin, vmax=vmax
    )
    ax.set_title(f"Detailed residual map - Lyon - XGBoost - ({cible})", fontsize=16)
    ax.axis("off")
    plt.tight_layout()
    plt.savefig(f"{FIG_DIR}/residus_lyon_{cible}.svg", dpi=600)
    plt.close()

    fig, ax = plt.subplots(figsize=(12, 10))
    gdf_lyon.plot(
        column="abs_residu", cmap="OrRd", legend=True,
        edgecolor="black", linewidth=0.2, ax=ax,
        legend_kwds={"label": "Absolute residual |actual - predicted|", "shrink": 0.7}
    )
    ax.set_title(f"Detailed absolute residual map - Lyon - XGBoost - ({cible})", fontsize=16)
    ax.axis("off")
    plt.tight_layout()
    plt.savefig(f"{FIG_DIR}/residus_abs_lyon_{cible}.svg", dpi=600)
    plt.close()

def modele_xgb(X, y, y_label, secteurs_uid):
    print(f"XGBoost for {y_label}...")

    xgb = XGBRegressor(n_estimators=100, learning_rate=0.1, max_depth=6, n_jobs=-1, random_state=42)
    xgb.fit(X, y)
    y_pred = xgb.predict(X)

    r2 = r2_score(y, y_pred)
    rmse = root_mean_squared_error(y, y_pred)

    print(f"{y_label.upper()} : R² = {r2:.3f}, RMSE = {rmse:.2f}")

    importances = pd.Series(xgb.feature_importances_, index=X.columns).sort_values(ascending=False)
    importances_df = importances.reset_index()
    importances_df.columns = ["variables", "importance"]
    importances_df.to_csv(f"{STATS_DIR}/importances_xgb_{y_label}.csv", index=False)

    plt.figure(figsize=(12, max(6, 0.4 * len(importances))))
    sns.barplot(x=importances.values, y=importances.index, hue=importances.index, dodge=False, palette="magma", legend=False)
    for i, (val, label) in enumerate(zip(importances.values, importances.index)):
        plt.text(val + 0.001, i, f"{val:.3f}", va='center')
    plt.title(f"Variable importance - XGBoost - ({y_label})", fontsize=16)
    plt.xlabel("Importance", fontsize=14)
    plt.ylabel("Variables", fontsize=14)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.tight_layout()
    plt.savefig(f"{FIG_DIR}/variable_importance_xgb_{y_label}.svg", dpi=600)
    plt.close()

    df_pred = pd.DataFrame({
        "secteur_uid": secteurs_uid,
        "reel": y,
        "pred": y_pred,
        "residu": y - y_pred,
        "abs_residu": (y - y_pred).abs()
    })
    out_parquet = f"{PREDICTION_DIR}/predictions_{y_label}.parquet"
    df_pred.to_parquet(out_parquet, index=False)

    carte_residus(df_pred, y_label)
    carte_residus_idf(df_pred, y_label)
    carte_residus_lyon(df_pred, y_label)

    return {"modele": "xgboost", "cible": y_label, "r2": r2, "rmse": rmse}


def analyse_xgboost():
    print("Loading data...")
    df = pd.read_csv(FUSION_PATH)

    X_cols = [col for col in df.columns if col not in ["secteur_uid", "population_jour", "population_nuit"]]
    X = df[X_cols]
    secteurs_uid = df["secteur_uid"]

    stats = []
    stats.append(modele_xgb(X, df["population_jour"], "population_jour", secteurs_uid))
    stats.append(modele_xgb(X, df["population_nuit"], "population_nuit", secteurs_uid))

    pd.DataFrame(stats).to_csv(f"{STATS_DIR}/xgboost_scores.csv", index=False)
    print("XGBoost completed: scores and maps generated.")


if __name__ == "__main__":
    analyse_xgboost()
