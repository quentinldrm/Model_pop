"""
Script : osm.py
Objective : Download OpenStreetMap (OSM) data for metropolitan France via Overpass,
            filtered by object types (buildings, shops, offices, services, leisure), and save them in GeoParquet format.
Author : LEDERMANN Quentin
Date : June 2025
Usage : This script is called by acquisition_pipeline.py to automate the acquisition of OSM data.
"""

import os
import requests
import geopandas as gpd
from typing import Optional, Literal
from modele.scripts.acquisition.acquisition_utils import load_config, print_status

# === SCRIPT PARAMETERS ===
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OUTPUT_DIR = "modele/data/osm"
REGIONS_GEOJSON = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/regions-version-simplifiee.geojson"
TAGS = ["building", "shop", "office", "amenity", "leisure"]
TAGS_EXTRA = ["building:levels", "building:use", "building:material", "building:roof:shape", "name", "operator"]
CRS = "EPSG:4326"

# Saves a GeoDataFrame in GeoParquet format with compression
def save_geoparquet(
    gdf: gpd.GeoDataFrame,
    path: str,
    compression: Optional[Literal['snappy', 'gzip', 'brotli']] = "brotli"
):
    # Saves spatial data with compression
    gdf.to_parquet(path, compression=compression, index=False)


# Retrieves the geographic bounds of metropolitan France from the simplified GeoJSON file
def get_bbox_france_metropolitaine():
    france = gpd.read_file(REGIONS_GEOJSON).to_crs(CRS)

    # INSEE codes for metropolitan regions only
    codes_metro = ["84", "27", "53", "24", "94", "44", "32", "11", "28", "75", "76", "52"]
    france_metro = france[france["code"].isin(codes_metro)]

    if france_metro.empty:
        raise ValueError("Metropolitan France not found.")
    return france_metro.total_bounds


# Builds an Overpass query (in OverpassQL language) for the given tags and bbox
def build_overpass_query(bbox):
    minx, miny, maxx, maxy = bbox
    bbox_str = f"{miny},{minx},{maxy},{maxx}"

    query = "[out:json][timeout:180];(\n"
    for tag in TAGS:
        query += f'  node["{tag}"]({bbox_str});\n'
        query += f'  way["{tag}"]({bbox_str});\n'
        query += f'  relation["{tag}"]({bbox_str});\n'
    query += ");\nout body;\n>;\nout skel qt;"
    return query


# Sends the Overpass query and retrieves OSM data in JSON format
def fetch_osm_data(query):
    response = requests.post(OVERPASS_URL, data={"data": query}, timeout=180)
    response.raise_for_status()
    return response.json()


# Converts OSM JSON elements into GeoDataFrames, grouped by main tag
def overpass_to_geodataframes_by_tag(data):
    elements = data.get("elements", [])
    nodes = {el["id"]: el for el in elements if el["type"] == "node"}

    grouped = {tag: [] for tag in TAGS}

    for el in elements:
        if "tags" not in el:
            continue

        # Détecte le tag principal de l'élément
        tag_type = next((k for k in TAGS if k in el["tags"]), None)
        if tag_type is None:
            continue

        # Crée la géométrie : Point pour node, Polygon pour way
        if el["type"] == "node":
            geom = {"type": "Point", "coordinates": [el["lon"], el["lat"]]}
        elif el["type"] == "way":
            coords = [
                [nodes[nid]["lon"], nodes[nid]["lat"]]
                for nid in el.get("nodes", [])
                if nid in nodes
            ]
            if len(coords) < 3:
                continue  # Pas suffisant pour un polygone
            geom = {"type": "Polygon", "coordinates": [coords]}
        else:
            continue

        # Ne conserve que les tags intéressants
        filtered_tags = {k: v for k, v in el["tags"].items() if k in TAGS + TAGS_EXTRA}

        grouped[tag_type].append({
            "type": "Feature",
            "geometry": geom,
            "properties": filtered_tags
        })

    # Convertit chaque groupe en GeoDataFrame
    gdfs = {
        tag: gpd.GeoDataFrame.from_features(features, crs=CRS)
        for tag, features in grouped.items() if features
    }
    return gdfs


# Main function: chains the retrieval and saving of OSM data
def download_osm():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print_status("Downloading OSM data", "info")

    try:
        bbox = get_bbox_france_metropolitaine()
        query = build_overpass_query(bbox)
        data = fetch_osm_data(query)
        gdfs_by_tag = overpass_to_geodataframes_by_tag(data)

        # Save filtered GeoDataFrames
        for tag, gdf in gdfs_by_tag.items():
            if not gdf.empty:
                output_path = os.path.join(OUTPUT_DIR, f"{tag}.parquet")
                save_geoparquet(gdf, output_path)
                print_status(f"Tag {tag} saved", "ok")

        print_status("OSM download completed", "ok")

    except Exception as e:
        print_status("OSM error", "err", str(e))


# Entry point
if __name__ == "__main__":
    download_osm()
