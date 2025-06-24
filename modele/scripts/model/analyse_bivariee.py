import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import numpy as np

# Load the dataset
file_path = "modele/output/fusion/features_modele.csv"
data = pd.read_csv(file_path)

# Define target variables
target_variables = ["population_jour", "population_nuit"]

# Compute R² and RMSE using linear regression (x → y)
results = []
for target in target_variables:
    for column in data.columns:
        if column not in target_variables and column != "secteur_uid":
            x = data[[column]]  # 2D array for sklearn
            y = data[target]
            model = LinearRegression().fit(x, y)
            y_pred = model.predict(x)
            r2 = r2_score(y, y_pred)
            rmse = np.sqrt(mean_squared_error(y, y_pred))
            results.append({"Variable": column, "Target": target, "R²": r2, "RMSE": rmse})

# Save results to a CSV file
results_df = pd.DataFrame(results)
results_df.to_csv("modele/output/eval/bivariate_metrics.csv", index=False)

# Charger les résultats en DataFrame s'ils ne le sont pas déjà
results_df = pd.DataFrame(results)

for target in target_variables:
    df_target = results_df[results_df["Target"] == target]

    # Barplot R²
    plt.figure(figsize=(12, 6))
    sns.barplot(data=df_target, x="Variable", y="R²", palette="Blues_d")
    plt.title(f"R² pour chaque variable explicative ({target})")
    plt.xticks(rotation=45, ha="right")
    plt.gca().yaxis.set_major_formatter(mtick.FormatStrFormatter('%.2f'))
    plt.tight_layout()
    plt.savefig(f"modele/output/eval/r2_{target}.svg")
    plt.close()

    # Barplot RMSE
    plt.figure(figsize=(12, 6))
    sns.barplot(data=df_target, x="Variable", y="RMSE", palette="Reds_d")
    plt.title(f"RMSE pour chaque variable explicative ({target})")
    plt.xticks(rotation=45, ha="right")
    plt.gca().yaxis.set_major_formatter(mtick.FormatStrFormatter('%.2f'))
    plt.tight_layout()
    plt.savefig(f"modele/output/eval/rmse_{target}.svg")
    plt.close()


# Barplots combinés R² et RMSE
for target in target_variables:
    df_target = results_df[results_df["Target"] == target].sort_values("R²", ascending=False)

    fig, ax1 = plt.subplots(figsize=(14, 6))

    color_r2 = "tab:blue"
    color_rmse = "tab:red"

    # Axe pour R²
    ax1.set_title(f"R² et RMSE par variable explicative ({target})")
    ax1.set_xlabel("Variable")
    ax1.set_ylabel("R²", color=color_r2)
    ax1.bar(df_target["Variable"], df_target["R²"], color=color_r2, alpha=0.6, label="R²")
    ax1.tick_params(axis="y", labelcolor=color_r2)
    ax1.set_ylim(0, 1)
    ax1.set_xticklabels(df_target["Variable"], rotation=45, ha="right")

    # Axe pour RMSE
    ax2 = ax1.twinx()
    ax2.set_ylabel("RMSE", color=color_rmse)
    ax2.plot(df_target["Variable"], df_target["RMSE"], color=color_rmse, marker="o", label="RMSE")
    ax2.tick_params(axis="y", labelcolor=color_rmse)

    fig.tight_layout()
    plt.savefig(f"modele/output/eval/combined_metrics_{target}.svg")
    plt.close()
