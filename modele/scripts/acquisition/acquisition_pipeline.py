"""
Script : acquisition_pipeline.py
Objective : Orchestrate the sequential execution of all data acquisition scripts (SIRENE, BPE, OSM, Census, BD TOPO, Mobiliscope),
            and perform cache cleanup at the end of the process.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Master script of the acquisition module, to be run to retrieve all project data.
"""

import os
import sys
import shutil
import traceback

from modele.scripts.acquisition.sirene import download_sirene
from modele.scripts.acquisition.bpe import download_bpe
from modele.scripts.acquisition.osm import download_osm
from modele.scripts.acquisition.recens import download_recens
from modele.scripts.acquisition.topo import download_topo
from modele.scripts.acquisition.mobiliscope import download_mobiliscope
from modele.scripts.acquisition.acquisition_utils import print_status

# === GENERAL PARAMETERS ===
CACHE_DIR = "modele/cache"

# Executes a function while capturing potential errors
def safe_run(name, func):
    try:
        func()
    except Exception as e:
        print_status(name, "err", str(e))
        traceback.print_exc()


# Deletes the content of the cache folder at the end of the process
def clean_cache():
    if os.path.exists(CACHE_DIR):
        for filename in os.listdir(CACHE_DIR):
            file_path = os.path.join(CACHE_DIR, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print_status("Cache deletion error", "err", f"{file_path} : {e}")
        print_status("Cache folder cleaned", "ok")
    else:
        print_status("Cache folder not found", "info")


# Main function executing all acquisition modules
def main():
    print_status("=== STARTING DATA ACQUISITION ===", "info")
    safe_run("SIRENE Download", download_sirene)
    safe_run("BPE Download", download_bpe)
    safe_run("OSM Download", download_osm)
    safe_run("Census Download", lambda: download_recens(metropole=False))
    safe_run("BD TOPO Download", download_topo)
    safe_run("Mobiliscope Download", download_mobiliscope)
    clean_cache()
    print_status("=== DATA ACQUISITION COMPLETED ===", "ok")


# Entry point
if __name__ == "__main__":
    main()
