# Modeling Application

## Foreword

This folder is dedicated to applying the model. It is designed for a user-selected department and an adjustable grid size (from 200 meters to 1,000 meters).

## Folder Structure

```
ter-cartopop-ml/modele
├── cache/             # Temporary files
├── config/            # Configuration file
├── data/              # Global folder for used data
├── output/            # Results of processing and models
│   ├── features/           # Spatial variables generated for Mobiliscope sectors
│   ├── figures/            # Graphs, heatmaps, residual maps
│   ├── grid/               # Grid for Mobiliscope sectors
│   └── stats/              # Results of statistical analyses
├── scripts/           # Scripts organized by module
│   ├── acquisition/        # Scripts for data downloading
│   ├── features/           # Scripts for variable creation
│   ├── preprocess/         # Scripts for intermediate processing
│   └── results/            # Scripts for cartographic and statistical results
├── utils/             # Utilities
└── README_appli.md         # Information and usage guide for the application
```

## Minimum Configuration

Python 3.12.X or later

Microsoft Visual C++ 14.X or later

Disk Space: ~75 GB

Internet connection

<br><br>

# Data Acquisition

## Launching Automated Download

1. Specify the **DEPARTMENT** code and desired grid size **GRID SIZE** (from 100 to 1,000 meters) in `appli/config/settings.yaml`.

2. Run the acquisition pipeline:
```bash
python -m appli.scripts.acquisition.acquisition_pipeline
```
⚠️ May take several tens of minutes. In case of an error, it will display the error and the associated function.

## Results

Downloaded data is saved in GeoJSON format in the `appli/data` folder and its respective subfolders.

<br><br>

# Variable Construction

## Launching Preprocessing and Variable Creation

1. Run the preprocessing script:

```bash
python -m appli.scripts.preprocess.preprocess_pipeline
```

2. Run the variable creation script:

```bash
python -m appli.scripts.features.features_pipeline
```

3. Run the variable fusion script:

```bash
python -m appli.scripts.features.features_fusion
```

⚠️ May take several minutes. Presence of **FutureWarning** does not affect results.

## Results

Variables are created and saved in the `appli/output/features` folder.

<br><br>

# Model Application

## Cartography

This script outputs the results as files.

1. Run the results script:

```bash
python -m modele.scripts.train.train
```

⚠️ May take several minutes.

## Results

The output includes a file containing population predictions saved in the `appli/output/results` folder.

<br><br>

---



