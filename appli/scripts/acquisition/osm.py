"""
Script : osm.py
Objective : Download OpenStreetMap (OSM) data for a specific department using the Overpass API.
            Retrieve buildings, shops, offices, public amenities, and leisure facilities.
Author : LEDERMANN Quentin
Date : June 2025
Usage : Script for acquiring and preprocessing OSM data for a specific department.
"""

import os
import requests
import geopandas as gpd
from appli.scripts.acquisition.download_utils import load_config, print_status

# Function to load the geometry of the department from a GeoJSON file
def get_bbox_from_dept(dept_code):
    url = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/departements-version-simplifiee.geojson"
    france = gpd.read_file(url).to_crs("EPSG:4326")
    dept = france[france["code"] == dept_code]
    if dept.empty:
        raise ValueError(f"Department {dept_code} not found.")
    return dept.total_bounds

# Function to build an Overpass query to retrieve OSM data based on specified tags
def build_overpass_query(bbox):
    minx, miny, maxx, maxy = bbox
    bbox_str = f"{miny},{minx},{maxy},{maxx}"
    tags = ["building", "shop", "office", "amenity", "leisure"]
    query = "[out:json][timeout:180];(\n"
    for tag in tags:
        query += f'  node["{tag}"]({bbox_str});\n'
        query += f'  way["{tag}"]({bbox_str});\n'
        query += f'  relation["{tag}"]({bbox_str});\n'
    query += ");\nout body;\n>;\nout skel qt;"
    return query

# Function to send the query to the Overpass API and retrieve the data
def fetch_osm_data(query):
    url = "https://overpass-api.de/api/interpreter"
    response = requests.post(url, data={"data": query})
    response.raise_for_status()
    return response.json()

# Function to convert Overpass data into GeoDataFrames grouped by tag
def overpass_to_geodataframes_by_tag(data):
    elements = data.get("elements", [])
    nodes = {el["id"]: el for el in elements if el["type"] == "node"}

    tag_keys = ["building", "shop", "office", "amenity", "leisure"]
    tags_to_keep = tag_keys + [
        "building:levels", "building:use", "building:material", 
        "building:roof:shape", "name", "operator"
    ]

    grouped = {tag: [] for tag in tag_keys}

    for el in elements:
        if "tags" not in el:
            continue
        tag_type = next((k for k in tag_keys if k in el["tags"]), None)
        if tag_type is None:
            continue

        if el["type"] == "node":
            geom = {"type": "Point", "coordinates": [el["lon"], el["lat"]]}
        elif el["type"] == "way":
            coords = []
            for node_id in el.get("nodes", []):
                node = nodes.get(node_id)
                if node:
                    coords.append([node["lon"], node["lat"]])
            if len(coords) < 3:
                continue
            geom = {"type": "Polygon", "coordinates": [coords]}
        else:
            continue

        filtered_tags = {k: v for k, v in el["tags"].items() if k in tags_to_keep}
        feature = {
            "type": "Feature",
            "geometry": geom,
            "properties": filtered_tags
        }

        grouped[tag_type].append(feature)

    gdfs = {}
    for tag, features in grouped.items():
        if features:
            gdfs[tag] = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
    return gdfs

# Main function to download OSM data for a specific department
def download_osm():
    config = load_config()
    dept_code = config["departement"]
    output_dir = "appli/data/osm"
    os.makedirs(output_dir, exist_ok=True)

    print_status("Downloading OSM data", "info")
    bbox = get_bbox_from_dept(dept_code)
    query = build_overpass_query(bbox)
    data = fetch_osm_data(query)
    gdfs_by_tag = overpass_to_geodataframes_by_tag(data)

    for tag, gdf in gdfs_by_tag.items():
        output_path = os.path.join(output_dir, f"{tag}.geojson")
        gdf.to_file(output_path, driver="GeoJSON")
        print_status(f"File {tag}", "ok", f"{len(gdf)} objects")

    print_status("OSM data downloaded", "ok")

# Entry point
if __name__ == "__main__":
    download_osm()
