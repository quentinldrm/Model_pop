"""
Script : train.py
Objective : Train multiple models (Linear Regression, Random Forest, XGBoost) on population data (day/night),
            predict for a specific region, and export predictions as a GeoPackage.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for training and prediction in the modeling pipeline.
"""

import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import StandardScaler
import os
import geopandas as gpd
from appli.scripts.acquisition.download_utils import load_config, print_status

config = load_config()
DEPARTMENT_NUMBER = config["department"]

# === 1. Load training data ===
train_df = pd.read_csv("modele/output/fusion/features_modele.csv")  # contains target variables
X_cols = [col for col in train_df.columns if col not in ["secteur_uid", "population_jour", "population_nuit"]]
X_train = train_df[X_cols]
y_train_jour = train_df["population_jour"]
y_train_nuit = train_df["population_nuit"]

# === 2. Load features to predict (dynamic department) ===
test_df = pd.read_csv(f"appli/output/fusion/features_fusionnees_{DEPARTMENT_NUMBER}m.csv")
test_df["idINSPIRE"] = test_df["idINSPIRE"].astype(str)

# Harmonize column names
rename_mapping = {col.replace("_secteur", ""): col for col in train_df.columns if "_secteur" in col}
rename_mapping.update({
    "part_population_active": "part_actifs_secteur"  # Specific mapping
})
test_df.rename(columns=rename_mapping, inplace=True)

X_test = test_df.set_index("idINSPIRE")

# Check for missing columns
missing_cols = set(X_cols) - set(X_test.columns)

X_test = X_test.reindex(columns=X_cols).fillna(0)  # keep the same order, handle NaN values

# === 3. Normalize data ===
scaler = StandardScaler()
X_train = pd.DataFrame(scaler.fit_transform(X_train), columns=X_cols)
X_test = pd.DataFrame(scaler.transform(X_test), columns=X_cols, index=X_test.index)

# === 4. Train models ===
print("Training models...")

# LinearRegression
lr_jour = LinearRegression()
lr_nuit = LinearRegression()
lr_jour.fit(X_train, y_train_jour)
lr_nuit.fit(X_train, y_train_nuit)

# RandomForestRegressor
rf_jour = RandomForestRegressor()
rf_nuit = RandomForestRegressor()
rf_jour.fit(X_train, y_train_jour)
rf_nuit.fit(X_train, y_train_nuit)

# XGBRegressor
xgb_jour = XGBRegressor()
xgb_nuit = XGBRegressor()
xgb_jour.fit(X_train, y_train_jour)
xgb_nuit.fit(X_train, y_train_nuit)

# === 5. Predict for Bas-Rhin (department 67) ===
results = pd.DataFrame(index=X_test.index)

# Check columns in X_test before prediction
print("Columns used for predictions:")
print(X_test.columns)

results["prediction_lr_jour"] = lr_jour.predict(X_test)
results["prediction_lr_nuit"] = lr_nuit.predict(X_test)
results["prediction_rf_jour"] = rf_jour.predict(X_test)
results["prediction_rf_nuit"] = rf_nuit.predict(X_test)
results["prediction_xgb_jour"] = xgb_jour.predict(X_test)
results["prediction_xgb_nuit"] = xgb_nuit.predict(X_test)

# Check prediction results
print("Preview of predictions:")
print(results.describe())

# === 6. Spatial join with geographic grid ===
print("Performing spatial join...")
grid = gpd.read_file(f"appli/output/grid/grid_{DEPARTMENT_NUMBER}_200m.geojson")
grid["idINSPIRE"] = grid["idINSPIRE"].astype(str)

gdf_result = grid.merge(results, on="idINSPIRE", how="left")

# === 7. Export GeoPackage ===
output_path = f"appli/output/results/predictions_{DEPARTMENT_NUMBER}.gpkg"
gdf_result.to_file(output_path, layer="population_predictions", driver="GPKG")
print(f"GeoPackage export completed: {output_path}")