# Dynamic Population Mapping Model

**Machine learning-based population distribution estimation**  
_Master 1 OTG (Geomatics and Earth Obersvation) • University of Strasbourg • 2024-2025_

---

## Summary

This project provides a **dynamic population mapping model** using **varied spatial open-source data** and **machine learning methods** to estimate population distribution at fine spatial and temporal scales.

The model is generated and trained on Mobiliscope sectors at the national scale, then applied to a chosen territory with a user-defined grid size. The application is performed at the **departmental scale** with an **adjustable grid size** (from 200 to 1,000 meters).

The project was developed as part of the **TER (Supervised Research Project)** at the University of Strasbourg, focusing on innovative approaches to dynamic population cartography.

---

## Features

The project offers two main modules:

### 1. **Model Training** (`/modele`)

- National-scale model training using Mobiliscope sectors
- Machine learning algorithms (Random Forest, XGBoost, Regression)
- Principal Component Analysis (PCA) for feature reduction
- Model comparison and validation tools
- Bivariate analysis capabilities

### 2. **Model Application** (`/appli`)

- Departmental-scale population estimation
- Adjustable grid size (200m to 1,000m)
- Automated data acquisition pipeline
- Feature engineering and preprocessing
- Population distribution mapping

---

## Data Sources

### Spatial Data

| Database Name                             | Description                                                         | Format(s)             | Update Frequency | Source / Link                                                   | Available Spatial Level          |
| ----------------------------------------- | ------------------------------------------------------------------- | --------------------- | ---------------- | --------------------------------------------------------------- | -------------------------------- |
| **BD TOPO (IGN)**                         | Geographic reference: buildings, roads, points of interest          | Shapefile, GeoPackage | Quarterly        | [IGN](https://geoservices.ign.fr/bdtopo)                        | National, Regional, Departmental |
| **OpenStreetMap (OSM)**                   | Open participatory data on POI, buildings                           | .pbf, GeoJSON         | Continuous       | [Geofabrik](https://download.geofabrik.de/)                     | Highly detailed (coordinates)    |
| **Base SIRENE (INSEE)**                   | Location of all active companies and establishments                 | CSV, API              | Daily            | [INSEE SIRENE](https://www.sirene.fr/sirene/public/)            | Address, municipality            |
| **Base Permanente des Équipements (BPE)** | Distribution of public facilities (health, education, sports, etc.) | CSV                   | Annual           | [INSEE](https://www.insee.fr/fr/statistiques/3568638)           | Municipality, IRIS               |
| **Mobiliscope**                           | Hourly population estimates (daily mobility)                        | Interface, CSV        | Based on surveys | [Mobiliscope](https://mobiliscope.cnrs.fr/fr)                   | IRIS, grid (200m)                |
| **Census**                                | Social and fiscal data from the population census                   | CSV                   | Based on census  | [INSEE](https://www.insee.fr/fr/metadonnees/source/serie/s1321) | Grid (200m)                      |

---

## Methodology

### Feature Engineering

The model uses 14 carefully engineered features to capture urban structure and population distribution patterns:

| Feature Name                | Description                                                                                 | Source                                   | Type         | Calculation Method                                                             |
| :-------------------------- | :------------------------------------------------------------------------------------------ | :--------------------------------------- | :----------- | :----------------------------------------------------------------------------- |
| score_poi_pondere           | Cumulative attractiveness score of POIs weighted by type of facility (e.g., school > bench) | OpenStreetMap (amenity, shop, office...) | Quantitative | Weight assignment by POI type followed by weighted summation in each grid cell |
| emplois_estimes_pondere     | Estimated number of jobs weighted by economic activity (via NAF code)                       | SIRENE                                   | Quantitative | Filtering establishments by sector + weighting via average jobs (INSEE)        |
| densite_etablissements      | Number of establishments relative to built surface area in the grid cell                    | SIRENE, BD TOPO                          | Quantitative | nb_etabs_sirene / surf_batie                                                   |
| densite_commerces           | Commercial intensity per unit of built surface area                                         | SIRENE (NAF code), BD TOPO               | Quantitative | nb_commerces / surf_batie                                                      |
| indice_mixite_fonctionnelle | Activity diversity index (offices, shops, schools, etc.)                                    | SIRENE (NAF codes)                       | Quantitative | Shannon entropy on NAF categories per grid cell                                |
| part_population_active      | Proportion of active population in the grid cell                                            | INSEE Census                             | Quantitative | population 18-64 years / total population                                      |
| part_jeunes                 | Proportion of young people (under 25 years old)                                             | INSEE Census                             | Quantitative | population < 25 years / total population                                       |
| shape_index_moyen           | Average compactness index of buildings in the grid cell (P² / 4πA)                          | BD TOPO or OSM buildings                 | Quantitative | Calculate shape index per building, average per grid cell                      |
| hauteur_ponderee_surface    | Average height weighted by built surface area in the grid cell                              | BD TOPO, calculated surface              | Quantitative | Sum(H × A) / Sum(A)                                                            |
| ecart_type_hauteur          | Standard deviation of building heights in the grid cell                                     | OSM or BD TOPO (height or levels)        | Quantitative | Standard deviation of heights per grid cell                                    |
| ecart_type_surface_batiment | Standard deviation of building surface areas in the grid cell                               | BD TOPO or OSM                           | Quantitative | Standard deviation of `geometry.area` in the grid cell                         |
| distance_moyenne_batiments  | Average distance between buildings in the grid cell                                         | BD TOPO or OSM (points/centroids)        | Quantitative | Average distances to nearest neighbors                                         |
| volume_moyen_batiments      | Average volume of buildings in the grid cell                                                | BD TOPO                                  | Quantitative | Average of building volumes                                                    |
| largeur_moyenne_rue         | Average street width in the grid cell                                                       | BD TOPO                                  | Quantitative | Average width of streets contained in the grid cell                            |

### Machine Learning Models

The project implements multiple algorithms for comparison and optimization:

- **Random Forest**: Ensemble method for robust predictions
- **XGBoost**: Gradient boosting for high performance
- **Regression**: Baseline linear models
- **PCA Integration**: Dimensionality reduction for improved efficiency

### Technical Implementation

- **Backend**: Python 3.12+
- **Key Libraries**:
  - GeoPandas for spatial data manipulation
  - Scikit-learn for machine learning
  - XGBoost for gradient boosting
  - PyOsmium for OSM data processing
- **Data Processing**: Automated acquisition and preprocessing pipelines
- **Performance**: Optimized for large-scale national datasets

---

## System Requirements

### Minimum Configuration

- **Python**: 3.12.X or later
- **Microsoft Visual C++**: 14.X or later
- **Disk Space**:
  - Model application: ~75 GB
  - Model training: ~200 GB
- **Internet Connection**: Required for data acquisition

---

## Getting Started

### Prerequisites

- Python 3.12 or later installed
- Microsoft Visual C++ 14.X or later
- Sufficient disk space (see System Requirements)
- Internet connection for data downloads

### Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/Model_pop.git
cd Model_pop
```

2. Create and initialize the environment:

```bash
python -m setup_env
```

This script will:

- Create a virtual environment
- Install all required dependencies from requirements.txt
- Configure the project structure

### Usage

The project is divided into two main workflows:

#### Model Training (`/modele`)

For detailed instructions on training the model at national scale, see [modele/README_modele.md](modele/README_modele.md)

#### Model Application (`/appli`)

For detailed instructions on applying the model to a specific department, see [appli/README_appli.md](appli/README_appli.md)

### File Structure

```
Model_pop/
├── README.md              # Main documentation
├── requirements.txt       # Python dependencies
├── setup_env.py          # Environment setup script
├── appli/                # Model application module
│   ├── README_appli.md
│   ├── config/          # Configuration files
│   ├── scripts/         # Application scripts
│   │   ├── acquisition/ # Data download
│   │   ├── preprocess/  # Data preprocessing
│   │   └── features/    # Feature engineering
│   └── utils/           # Utility files
└── modele/              # Model training module
    ├── README_modele.md
    ├── scripts/         # Training scripts
    │   ├── acquisition/ # Data download
    │   ├── preprocess/  # Data preprocessing
    │   ├── features/    # Feature engineering
    │   ├── model/       # ML algorithms
    │   └── train/       # Training pipeline
    └── utils/           # Utility files
```

---

## Team

**Master 1 OTG - University of Strasbourg (2024-2025)**

- Quentin Ledermann

**Supervised by:**

- Kenji Fujiki
- Romain Wenger

---

## License

This project is part of an academic work at the University of Strasbourg. For any use or reproduction, please contact the authors.

---

## Acknowledgments

- **University of Strasbourg** - Master OTG program
- **IGN** (French National Geographic Institute) for geographic data
- **INSEE** for census and establishment data
- **OpenStreetMap** community for open geospatial data
- **CNRS Mobiliscope** for mobility and population data

---

## Contact

For questions or collaboration opportunities, please contact:

- University of Strasbourg - Master 1 OTG
- Email: quentinledermann@outlook.fr

---

**© 2024-2025 - TER Project - University of Strasbourg**
