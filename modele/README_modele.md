# Modeling

## Foreword

This folder is dedicated to model creation. It is based on Mobiliscope sectors in metropolitan France.

## Folder Structure

```
ter-cartopop-ml/modele
├── cache/             # Temporary files
├── data/              # Global folder for used and generated data
│   ├── mobiliscope/        # Reference data
│   ├── processed/          # Reprojected, filtered, or cleaned data
│   ├── raw/                # Raw downloaded data
│   └── target              # Model target data
├── output/            # Results of processing and models
│   ├── features/           # Spatial variables generated for Mobiliscope sectors
│   ├── figures/            # Graphs, heatmaps, residual maps
│   ├── grid/               # Grid for Mobiliscope sectors
│   └── stats/              # Results of statistical analyses
├── scripts/           # Scripts organized by module
│   ├── acquisition/        # Scripts for data downloading
│   ├── features/           # Scripts for variable creation
│   ├── model/              # Scripts for model creation
│   └── preprocess/         # Scripts for intermediate processing
├── utils/             # Utilities
└── README_modele.md        # Information and usage guide for modeling
```

## Minimum Configuration

Python 3.12.X or later

Microsoft Visual C++ 14.X or later

Disk Space: ~200 GB

Internet connection

<br><br>

# Data Acquisition

## Launching Automated Download

1. Run the acquisition pipeline:
```bash
python -m modele.scripts.acquisition.acquisition_pipeline
```
⚠️ May take several hours. In case of an error, it will display the error and the associated function.

## Results

Downloaded data is saved in GeoJSON format in the **data** folder and its respective subfolders.

<br><br>

# Variable Construction

## Launching Preprocessing and Variable Creation

1. Run the preprocessing script:

```bash
python -m modele.scripts.preprocess.preprocess_pipeline
```

2. Run the variable creation script:

```bash
python -m modele.scripts.features.features_pipeline
```

3. Run the variable fusion script:

```bash
python -m modele.scripts.features.features_fusion
```

⚠️ May take several hours. Presence of **FutureWarning** does not affect results.

## Results

Variables are created and saved in the **output/features** folder.

<br><br>

# Statistical Tests and Model

## Statistical Analysis

This script calculates various statistical indicators: R², RMSE, and also generates spatial residual maps.

1. Run the model analysis script:

```bash
python -m modele.scripts.modele.modele_pipeline
```

⚠️ May take several minutes.

## Results

The output includes indicators, a heatmap, and a scatter plot. All are saved in the **modele/output/modele/stats** & **modele/output/modele/figures** folders.

<br><br>

---



