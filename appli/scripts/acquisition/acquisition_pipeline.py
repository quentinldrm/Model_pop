import sys
import traceback
import os 
import shutil

from appli.scripts.acquisition.sirene import download_sirene
from appli.scripts.acquisition.bpe import download_bpe
from appli.scripts.acquisition.osm import download_osm
from appli.scripts.acquisition.recens import download_recens
from appli.scripts.acquisition.topo import download_topo
from appli.scripts.acquisition.download_utils import print_status

def safe_run(name, func):
    try:
        func()
    except Exception as e:
        print_status(name, "err", str(e))
        traceback.print_exc()

def clean_cache():
    cache_dir = "cache"
    if os.path.exists(cache_dir):
        for filename in os.listdir(cache_dir):
            file_path = os.path.join(cache_dir, filename)
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

def main():
    print("=== STARTING DATA ACQUISITION ===\n")
    safe_run("SIRENE Download", download_sirene)
    safe_run("BPE Download", download_bpe)
    safe_run("OSM Download", download_osm)
    safe_run("Census Download", download_recens)
    safe_run("BD TOPO Download", download_topo)
    clean_cache()
    print("\n=== DATA ACQUISITION COMPLETED ===")


if __name__ == "__main__":
    main()