"""
Script : acp.py (enhanced version)
Objective : PCA with improved visualizations (style, readability, HD export)
Author : LEDERMANN Quentin + ChatGPT
Date : June 2025
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from matplotlib.patches import Circle
import os

# === STYLES ===
sns.set_theme(style="white")
plt.rcParams.update({"font.size": 12})

# === PATHS ===
INPUT_PATH = "modele/output/fusion/variables_merged.csv"
FIG_DIR = "modele/output/acp/figures"
SCORES_PATH = "modele/output/acp/resultats_acp.csv"
os.makedirs(FIG_DIR, exist_ok=True)

# === DATA LOADING ===
df = pd.read_csv(INPUT_PATH)
id_col = "idINSPIRE"
X = df.drop(columns=[id_col])
ids = df[id_col]

# === STANDARDIZATION ===
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# === PCA ===
pca = PCA()
X_pca = pca.fit_transform(X_scaled)
components = pca.components_.T[:, :2]

# === SCREE PLOT ===
plt.figure(figsize=(10, 6))
plt.plot(range(1, len(pca.explained_variance_ratio_) + 1),
         pca.explained_variance_ratio_, marker='o', label='Explained variance')
plt.plot(range(1, len(pca.explained_variance_ratio_) + 1),
         np.cumsum(pca.explained_variance_ratio_), marker='s', label='Cumulative variance')
plt.axhline(y=0.8, color='red', linestyle='--', label='80% threshold')
plt.xlabel('Principal components')
plt.ylabel('Variance proportion')
plt.title('PCA - Variance explained by component')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig(f"{FIG_DIR}/01_scree_plot_variance_expliquee.svg", dpi=600)
plt.close()

# === CORRELATION CIRCLE: FULL + 2 ZOOMS ===
def plot_correlation_circle(ax, zoom=False):
    ax.axhline(0, color='grey', lw=1)
    ax.axvline(0, color='grey', lw=1)
    circle = Circle((0, 0), 1, edgecolor='black', facecolor='none', linestyle='--')
    ax.add_patch(circle)
    for i, (x, y) in enumerate(components):
        ax.arrow(0, 0, x, y, color='tab:blue', alpha=0.6, head_width=0.02)
        if not zoom or (abs(x) < 0.55 and abs(y) < 0.55):
            ax.text(x * 1.08, y * 1.08, X.columns[i], ha='center', va='center', fontsize=9)
    ax.set_xlabel(f"PC1 ({pca.explained_variance_ratio_[0]*100:.1f}%)")
    ax.set_ylabel(f"PC2 ({pca.explained_variance_ratio_[1]*100:.1f}%)")
    ax.grid(True)
    if zoom:
        ax.set_xlim(-0.3, 0.6)
        ax.set_ylim(-0.3, 0.6)
    else:
        ax.set_xlim(-1.1, 1.1)
        ax.set_ylim(-1.1, 1.1)
    ax.set_aspect('equal')

fig, axs = plt.subplots(1, 2, figsize=(16, 8))
plot_correlation_circle(axs[0])
plot_correlation_circle(axs[1], zoom=True)
fig.suptitle("PCA - Correlation circle (PC1 vs PC2)", fontsize=14)
fig.tight_layout(rect=(0, 0, 1, 0.96))
fig.savefig(f"{FIG_DIR}/02_cercle_corre_PC1_PC2.svg", dpi=600)
plt.close()

# === BIPLOT OBSERVATIONS (WITHOUT VECTORS) ===
fig, ax = plt.subplots(figsize=(10, 8))
ax.scatter(X_pca[:, 0], X_pca[:, 1], alpha=0.4, s=10, c='tab:blue')
ax.set_xlabel("PC1")
ax.set_ylabel("PC2")
ax.set_title("Projection of observations (PCA)", fontsize=14)
fig.tight_layout()
fig.savefig(f"{FIG_DIR}/03_biplot_observations_PC1_PC2.svg", dpi=600)
plt.close()

# === VARIABLE CONTRIBUTIONS PC1 & PC2 ===
for i in range(2):
    fig, ax = plt.subplots(figsize=(12, 6))
    contrib = np.abs(pca.components_[i])
    sorted_idx = np.argsort(contrib)[::-1]
    ax.bar([X.columns[j] for j in sorted_idx], contrib[sorted_idx], color="steelblue")
    ax.set_title(f"Variable contributions to PC{i+1}", fontsize=14)
    ax.set_ylabel("Absolute contribution")
    ax.tick_params(axis='x', rotation=90)
    fig.tight_layout()
    fig.savefig(f"{FIG_DIR}/04_contributions_PC{i+1}.svg", dpi=600)
    plt.close()

# === EXPORT SCORES ===
scores = pd.DataFrame(X_pca, columns=[f"PC{i+1}" for i in range(X_pca.shape[1])])
scores[id_col] = ids
scores.to_csv(SCORES_PATH, index=False)

print("PCA graphs exported to modele/output/acp/figures/")
