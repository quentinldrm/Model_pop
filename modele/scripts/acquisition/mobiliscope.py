"""
Script : mobiliscope.py
Objective : Download and extract Mobiliscope data (July 2023 version),
            filtered to retain only cities in metropolitan France.
Author : LEDERMANN Quentin
Date : June 2025
Usage : This script is called by acquisition_pipeline.py to automate the acquisition of Mobiliscope.
"""

import os
import requests
from io import BytesIO
import zipfile
import shutil
from modele.scripts.acquisition.acquisition_utils import load_config, print_status

# === SCRIPT PARAMETERS ===
MOBILISCOPE_URL = "https://zenodo.org/records/11111161/files/20230706opendata.zip?download=1"
CACHE_DIR = "modele/cache"
FINAL_DIR = "modele/data/mobiliscope"
EXTRACTED_FOLDER_NAME = "20230706_opendata"
EXTRACTED_PATH = os.path.join(CACHE_DIR, EXTRACTED_FOLDER_NAME)

# List of cities to exclude (outside metropolitan France or irrelevant)
EXCLUDE_CITIES = [
    "bogota", "idf2020", "la-reunion", "martinique", "montreal", "ottawa-gatineau",
    "quebec", "saguenay", "santiago", "sao-paulo", "sherbrooke", "trois-rivieres",
    "valenciennes2011",
]


# Moves GeoJSON and CSV files of interest from subfolders of each city to the final folder
def move_files_from_france(extract_dir, final_dir):
    for city in os.listdir(extract_dir):
        city_path = os.path.join(extract_dir, city)

        if not os.path.isdir(city_path):
            continue  # Ignore files

        if city.lower() in EXCLUDE_CITIES:
            continue  # Ignore cities outside metropolitan France

        # Search for files of interest
        geojson_src = os.path.join(city_path, "layers", "secteurs.geojson")
        csv_src = os.path.join(city_path, "stacked", "pop_choro_stacked.csv")

        # Copy files if they exist
        if os.path.exists(geojson_src):
            shutil.copy2(geojson_src, os.path.join(final_dir, f"{city.lower()}_secteurs.geojson"))

        if os.path.exists(csv_src):
            shutil.copy2(csv_src, os.path.join(final_dir, f"{city.lower()}_pop_choro_stacked.csv"))


# Downloads the Mobiliscope archive, extracts it, and filters useful files
def download_mobiliscope():
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(FINAL_DIR, exist_ok=True)

    # If the archive is already extracted, proceed directly to file selection
    if os.path.isdir(EXTRACTED_PATH):
        print_status("Mobiliscope already extracted - extraction skipped", "info")
        move_files_from_france(EXTRACTED_PATH, FINAL_DIR)
        return

    print_status("Downloading Mobiliscope", "info")
    try:
        # Download the ZIP file
        response = requests.get(MOBILISCOPE_URL, timeout=30)
        response.raise_for_status()

        # Extract into the cache folder
        with zipfile.ZipFile(BytesIO(response.content)) as archive:
            archive.extractall(path=CACHE_DIR)

        print_status("Extraction completed", "ok")

        # Move useful files
        move_files_from_france(EXTRACTED_PATH, FINAL_DIR)
        print_status("Mobiliscope processed and filtered", "ok")

    except requests.RequestException as e:
        print_status("Downloading Mobiliscope", "err", str(e))

    except Exception as e:
        print_status("Error during Mobiliscope processing", "err", str(e))


# Entry point if the script is run directly
if __name__ == "__main__":
    download_mobiliscope()
