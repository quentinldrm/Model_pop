"""
Script : comparaison_modeles.py
Objective : Visualize the comparative performance of models (R² and RMSE)
Author : LEDERMANN Quentin + ChatGPT
Date : June 2025
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.container as container
import seaborn as sns
import os

# === PATHS TO FILES ===
REGRESSION_PATH = "modele/output/regression/stats_regression.csv"
RF_PATH = "modele/output/random_forest/scores_random_forest.csv"
XGB_PATH = "modele/output/xgboost/scores_xgboost.csv"
FIG_DIR = "modele/output/eval"
os.makedirs(FIG_DIR, exist_ok=True)

# === DATA LOADING ===
regression_df = pd.read_csv(REGRESSION_PATH)
rf_df = pd.read_csv(RF_PATH)
xgb_df = pd.read_csv(XGB_PATH)

# Add model name if missing
rf_df["modele"] = "random_forest"
xgb_df["modele"] = "xgboost"

# Harmonize column names
rf_df["cible"] = rf_df["cible"].replace({"population_jour": "pop_jour", "population_nuit": "pop_nuit"})
xgb_df["cible"] = xgb_df["cible"].replace({"population_jour": "pop_jour", "population_nuit": "pop_nuit"})
rf_df["cible"] = rf_df["cible"].replace({"population_jour": "pop_jour", "population_nuit": "pop_nuit"})
xgb_df["cible"] = xgb_df["cible"].replace({"population_jour": "pop_jour", "population_nuit": "pop_nuit"})

# Merge
df = pd.concat([regression_df, rf_df, xgb_df], ignore_index=True)

# Sort
df = df.sort_values(by=["cible", "modele"])

# === CUSTOM COLORS AND LABELS ===
etiquettes_modele = {
    "regression": "Linear Regression",
    "random_forest": "Random Forest",
    "xgboost": "XGBoost"
}
palette = {
    "Linear Regression": "#56c2fc",
    "Random Forest": "#2f7625",
    "XGBoost": "#fabd53"
}
df["modele"] = df["modele"].replace(etiquettes_modele)

# === VISUALIZATION ===
sns.set_theme(style="whitegrid")
fig, axes = plt.subplots(1, 2, figsize=(16, 8))

# R²
r2_plot = sns.barplot(data=df, x="cible", y="r2", hue="modele", ax=axes[0], palette=palette)
axes[0].set_title("R² Score by Model and Target Variable", fontsize=16)
axes[0].set_ylabel("Coefficient of Determination R²", fontsize=12)
axes[0].set_xlabel("Target Variable", fontsize=12)
axes[0].tick_params(labelsize=11)
axes[0].legend(loc="lower right", title="Model", fontsize=11, title_fontsize=12)

from matplotlib.container import BarContainer

for container in r2_plot.containers:
    if isinstance(container, BarContainer):
        r2_plot.bar_label(container, fmt="%.3f", label_type="edge", fontsize=11, padding=3)

# RMSE
rmse_plot = sns.barplot(data=df, x="cible", y="rmse", hue="modele", ax=axes[1], palette=palette)
axes[1].set_title("Root Mean Square Error (RMSE)", fontsize=16)
axes[1].set_ylabel("RMSE (Mean Absolute Error)", fontsize=12)
axes[1].set_xlabel("Target Variable", fontsize=12)
for container in rmse_plot.containers:
    if isinstance(container, BarContainer):
        rmse_plot.bar_label(container, fmt="%.0f", label_type="edge", fontsize=11, padding=3)

plt.tight_layout(pad=3.0)
plt.savefig(os.path.join(FIG_DIR, "comparaison_modeles_r2_rmse.svg"), dpi=600)
print("Graph saved in:", FIG_DIR)

# === CREATION OF SUMMARY TABLE ===
from matplotlib.table import Table

# Prepare data for the table
table_data = df.groupby("modele")[["r2", "rmse"]].mean().reset_index()
table_data.columns = ["Model", "Mean R²", "Mean RMSE"]

# Round numbers to 3 decimals for R² and 0 decimals for RMSE
table_data["Mean R²"] = table_data["Mean R²"].round(3)
table_data["Mean RMSE"] = table_data["Mean RMSE"].round(0)

# Create figure for the table
fig_table, ax_table = plt.subplots(figsize=(8, 4))
ax_table.axis("tight")
ax_table.axis("off")

# Add table with colors for rows in the first column
table = ax_table.table(
    cellText=table_data.values.tolist(),
    colLabels=table_data.columns.tolist(),
    cellLoc="center",
    loc="center"
)
table.auto_set_font_size(False)
table.set_fontsize(12)
table.scale(1.2, 1.2)

# Define colors for models
row_colors = {
    "Random Forest": "#2f7625",  
    "Linear Regression": "#56c2fc",  
    "XGBoost": "#fabd53"  
}

# Apply colors to rows in the first column
for (row, col), cell in table.get_celld().items():
    if col == 0 and row > 0:  
        model_name = table_data.values[row - 1][0] 
        cell.set_facecolor(row_colors.get(model_name, "#ffffff"))  
        cell.set_text_props(color="white", weight="bold")  

# Add light gray color for column headers
for (row, col), cell in table.get_celld().items():
    if row == 0:  
        cell.set_facecolor("#d5d5d5") 
        cell.set_text_props(color="black", weight="bold")  

# Save the table as an image
plt.savefig(os.path.join(FIG_DIR, "tableau_recap.svg"), dpi=600)
print("Summary table saved in:", FIG_DIR)
