# TER - Dynamic Population Mapping Model

## Context

Project developed as part of the Master 1 OTG (University of Strasbourg).

This project aims to build a **dynamic population mapping model** using **varied spatial open-source data** and **machine learning methods**.

The model is generated and trained on Mobiliscope sectors at the national scale, then applied to a chosen territory with a user-defined grid size.

The model application is performed at the **departmental scale** with an **adjustable grid size** (from 200 to 1,000 meters).

## Project Structure

```
ter-cartopop-ml/
├── .venv/             # Virtual environment
├── appli/             # Model application folder
├── modele/            # Model creation folder
├── README.md          # Main information and usage guide
├── requirements.txt   # Python dependencies
└── setup_env.py       # Initializes the virtual environment and installs libraries
```

## Minimum Configuration

Python 3.12.X or later

Microsoft Visual C++ 14.X or later

Disk Space: ~75 GB for application and ~200 GB for modeling

Internet connection

<br><br>

# Data Used

| Database Name                                   | Description                                                                                   | Format(s)           | Update Frequency | Source / Link                                                   | Available Spatial Level        |
|------------------------------------------------|-----------------------------------------------------------------------------------------------|---------------------|------------------|------------------------------------------------------------------|-------------------------------|
| **BD TOPO (IGN)**                              | Geographic reference: buildings, roads, points of interest                                    | Shapefile, GeoPackage  | Quarterly        | [IGN](https://geoservices.ign.fr/bdtopo)                         | National, Regional, Departmental      |
| **OpenStreetMap (OSM)**                        | Open participatory data on POI, buildings                                                    | .pbf, GeoJSON       | Continuous       | [Geofabrik](https://download.geofabrik.de/)                     | Highly detailed (coordinates)      |
| **Base SIRENE (INSEE)**                        | Location of all active companies and establishments                                          | CSV, API            | Daily            | [INSEE SIRENE](https://www.sirene.fr/sirene/public/)            | Address, municipality          |
| **Base Permanente des Équipements (BPE)**      | Distribution of public facilities (health, education, sports, etc.)                          | CSV                 | Annual           | [INSEE](https://www.insee.fr/fr/statistiques/3568638)           | Municipality, IRIS             |
| **Mobiliscope**                                | Hourly population estimates (daily mobility)                                                 | Interface, CSV      | Based on surveys | [Mobiliscope](https://mobiliscope.cnrs.fr/fr)                   | IRIS, grid (200m)              |
| **Census**                                     | Social and fiscal data from the population census                                            | CSV                 | Based on census  | [INSEE](https://www.insee.fr/fr/metadonnees/source/serie/s1321) | Grid (200m)                    |

<br><br>

# List of Variables

| Feature Name               | Description                                                                           | Source                                   | Type         | Calculation Method                                                                   |
|:---------------------------|:--------------------------------------------------------------------------------------|:-----------------------------------------|:-------------|:------------------------------------------------------------------------------------|
| score_poi_pondere          | Cumulative attractiveness score of POIs weighted by type of facility (e.g., school > bench) | OpenStreetMap (amenity, shop, office...) | Quantitative | Weight assignment by POI type followed by weighted summation in each grid cell     |
| emplois_estimes_pondere    | Estimated number of jobs weighted by economic activity (via NAF code)                | SIRENE                                   | Quantitative | Filtering establishments by sector + weighting via average jobs (INSEE)            |
| densite_etablissements     | Number of establishments relative to built surface area in the grid cell              | SIRENE, BD TOPO                          | Quantitative | nb_etabs_sirene / surf_batie                                                        |
| densite_commerces          | Commercial intensity per unit of built surface area                                   | SIRENE (NAF code), BD TOPO               | Quantitative | nb_commerces / surf_batie                                                           |
| indice_mixite_fonctionnelle| Activity diversity index (offices, shops, schools, etc.)                              | SIRENE (NAF codes)                       | Quantitative | Shannon entropy on NAF categories per grid cell                                    |
| part_population_active     | Proportion of active population in the grid cell                                      | INSEE Census                             | Quantitative | population 18-64 years / total population                                           |
| part_jeunes                | Proportion of young people (under 25 years old)                                       | INSEE Census                             | Quantitative | population < 25 years / total population                                            |
| shape_index_moyen          | Average compactness index of buildings in the grid cell (P² / 4πA)                    | BD TOPO or OSM buildings                 | Quantitative | Calculate shape index per building, average per grid cell                          |
| hauteur_ponderee_surface   | Average height weighted by built surface area in the grid cell                        | BD TOPO, calculated surface              | Quantitative | Sum(H × A) / Sum(A)                                                                |
| ecart_type_hauteur         | Standard deviation of building heights in the grid cell                               | OSM or BD TOPO (height or levels)        | Quantitative | Standard deviation of heights per grid cell                                        |
| ecart_type_surface_batiment| Standard deviation of building surface areas in the grid cell                         | BD TOPO or OSM                           | Quantitative | Standard deviation of `geometry.area` in the grid cell                             |
| distance_moyenne_batiments | Average distance between buildings in the grid cell                                   | BD TOPO or OSM (points/centroids)        | Quantitative | Average distances to nearest neighbors                                             |
| volume_moyen_batiments     | Average volume of buildings in the grid cell                                          | BD TOPO                                  | Quantitative | Average of building volumes                                                        |
| largeur_moyenne_rue        | Average street width in the grid cell                                                 | BD TOPO                                  | Quantitative | Average width of streets contained in the grid cell                                |

<br><br>

# Project Configuration

1. Create and initialize the environment:
```bash
python -m setup_env
```

## Instructions

For modeling and application, each step is detailed in its respective README file.

<br><br>

---

Contact: Quentin Ledermann M1 OTG / Supervised by Kenji Fujiki and Romain Wenger

