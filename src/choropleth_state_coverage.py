# Copyright 2026 Alexander Kombeiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -*- coding: utf-8 -*-
"""
Created on 7/3/25
@AUTHOR: Alexander Kombeiz (akombeiz@ukaachen.de)
d@VERSION=2.0
"""

import json
import math
import zipfile
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import requests

from helper.paths import get_base_csv_file, get_output_dir, get_downloads_dir

GEOJSON_URL = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-germany-land/exports/geojson"
GEOJSON_FILENAME = "germany.geojson"

ZIPCODES_URL = "https://github.com/zauberware/postal-codes-json-xml-csv/raw/refs/heads/master/data/DE.zip"
ZIPCODES_ZIPNAME = "de.zip"
ZIPCODES_CSV_PATH = "zipcodes.de.csv"

ITERATIONS = 5
BASE_RADIUS_DEG = 0.06  # 0.06 deg is approx 6-7km in Germany
RADIUS_GROWTH_PER_SITE = 0.015  # 0.015 deg is approx 1.5km growth per added node


def prepare_data(csv_file: Path, download_dir: Path) -> tuple[pd.Series, pd.DataFrame]:
  df = pd.read_csv(csv_file)
  df["zipcode"] = df["zipcode"].astype(str).str.zfill(5).str.strip()
  if "state" in df.columns:
    df["state"] = df["state"].astype(str).str.strip()
  zipcode_path = download_file_if_needed(ZIPCODES_URL, download_dir, ZIPCODES_ZIPNAME)
  zipcode_df = load_csv_from_zip(zipcode_path, ZIPCODES_CSV_PATH)
  zipcode_df["zipcode"] = zipcode_df["zipcode"].astype(str).str.zfill(5).str.strip()
  zipcode_df = zipcode_df.drop_duplicates(subset=["zipcode"], keep="first")
  nodes_with_coords = df.merge(zipcode_df[["zipcode", "latitude", "longitude"]], on="zipcode", how="left")
  valid_nodes = nodes_with_coords.dropna(subset=["latitude", "longitude"]).copy()
  dropped_count = len(df) - len(valid_nodes)
  if dropped_count > 0:
    print(f"WARNING: {dropped_count} sites were dropped due to invalid/missing ZIP coordinates.")
  state_counts = valid_nodes["state"].value_counts()
  return state_counts, valid_nodes


def perform_iterative_aggregation(nodes_df: pd.DataFrame) -> pd.DataFrame:
  """
  Groups nodes using a graph-based connected components approach.
  Any nodes whose radii touch are merged into a single cluster
  """
  valid_nodes = nodes_df.dropna(subset=["latitude", "longitude"]).copy()
  current_nodes = []
  for _, row in valid_nodes.iterrows():
    current_nodes.append({"lat": row["latitude"], "lon": row["longitude"], "count": 1})

  for it in range(ITERATIONS):
    n = len(current_nodes)
    if n == 0:
      break

    # 1. Update Radii based on current counts
    for node in current_nodes:
      # Linear growth of radius based on count
      node["r"] = BASE_RADIUS_DEG + (node["count"] * RADIUS_GROWTH_PER_SITE)

    # 2. Build Adjacency List (Who touches whom?)
    adj = {i: [] for i in range(n)}

    for i in range(n):
      for j in range(i + 1, n):
        n1 = current_nodes[i]
        n2 = current_nodes[j]

        # Longitude correction (approx for Germany's latitude)
        # This prevents circles from looking like ovals in calculations
        mean_lat = (n1["lat"] + n2["lat"]) / 2.0
        lon_scale = math.cos(math.radians(mean_lat))

        d_lat = abs(n1["lat"] - n2["lat"])
        d_lon = abs(n1["lon"] - n2["lon"]) * lon_scale
        dist = math.sqrt(d_lat**2 + d_lon**2)

        # Check intersection: Distance < Sum of Radii
        if dist < (n1["r"] + n2["r"]):
          adj[i].append(j)
          adj[j].append(i)

    # 3. Find Connected Components (Merge Chains)
    visited = set()
    next_nodes = []

    for i in range(n):
      if i in visited:
        continue

      # BFS to find the whole cluster (chain of touching dots)
      cluster_indices = [i]
      stack = [i]
      visited.add(i)

      while stack:
        curr = stack.pop()
        for neighbor in adj[curr]:
          if neighbor not in visited:
            visited.add(neighbor)
            stack.append(neighbor)
            cluster_indices.append(neighbor)

      # 4. Create the Merged Node
      if len(cluster_indices) == 1:
        old = current_nodes[i]
        next_nodes.append({"lat": old["lat"], "lon": old["lon"], "count": old["count"]})
      else:
        sum_lat = 0.0
        sum_lon = 0.0
        total_count = 0

        for idx_c in cluster_indices:
          node = current_nodes[idx_c]
          w = node["count"]
          sum_lat += node["lat"] * w
          sum_lon += node["lon"] * w
          total_count += w

        next_nodes.append({"lat": sum_lat / total_count, "lon": sum_lon / total_count, "count": total_count})

    # Convergence Check
    if len(next_nodes) == len(current_nodes):
      print(f"Converged at iteration {it + 1}")
      break
    print(f"Iteration {it + 1}: Merged {len(current_nodes)} -> {len(next_nodes)} nodes")
    current_nodes = next_nodes
  return pd.DataFrame(current_nodes)


from shapely.geometry import Point


def plot_network_map(gdf: gpd.GeoDataFrame, nodes_df: pd.DataFrame, output_dir: Path):
  output_dir.mkdir(parents=True, exist_ok=True)
  fig, ax = plt.subplots(figsize=(12, 12))
  gdf_no_sites = gdf[gdf["site_count"] == 0]

  # --- 1. Draw Map Background ---
  gdf.plot(color="whitesmoke", edgecolor="#555555", linewidth=0.8, ax=ax, zorder=1)
  ax.axis("off")

  # Hatching for empty states
  if not gdf_no_sites.empty:
    gdf_no_sites.plot(color="none", edgecolor="red", hatch="///", linewidth=0.0, ax=ax, zorder=2)
    gdf_no_sites.plot(color="none", edgecolor="red", linewidth=0.8, ax=ax, zorder=3)

  # --- 2. Process and Plot Nodes ---
  agg_nodes = perform_iterative_aggregation(nodes_df)

  # --- CHANGE A: remove blue points in Berlin (keep only the white state count bubble) ---
  if not agg_nodes.empty:
    berlin_geom = gdf.loc[gdf["lan_name"] == "Berlin", "geometry"]
    if not berlin_geom.empty:
      berlin_geom = berlin_geom.iloc[0]

      pts = gpd.GeoDataFrame(
        agg_nodes.copy(), geometry=[Point(xy) for xy in zip(agg_nodes["lon"], agg_nodes["lat"])], crs=gdf.crs
      )
      # drop any aggregated nodes that are within Berlin
      pts = pts[~pts.within(berlin_geom)]
      agg_nodes = pd.DataFrame(pts.drop(columns=["geometry"]))

  if not agg_nodes.empty:
    agg_nodes["logic_radius"] = BASE_RADIUS_DEG + (agg_nodes["count"] * RADIUS_GROWTH_PER_SITE)
    sizes = (agg_nodes["logic_radius"] ** 2) * 25000

    ax.scatter(agg_nodes["lon"], agg_nodes["lat"], s=sizes, c="blue", alpha=0.6, edgecolors="white", linewidth=1.0, zorder=5)

    for _, row in agg_nodes.iterrows():
      if row["count"] > 1:
        ax.text(
          row["lon"],
          row["lat"],
          str(int(row["count"])),
          fontsize=9,
          ha="center",
          va="center",
          color="white",
          weight="bold",
          zorder=6,
        )

  # --- 3. State Count Labels ---
  for idx, row in gdf.iterrows():
    count = int(row["site_count"])
    if count <= 0:
      continue

    state = row.get("lan_name", "")
    centroid = row["geometry"].centroid

    # defaults
    fs = 14
    pad = 0.3
    xy = (centroid.x, centroid.y)

    # --- CHANGE B: Sachsen label spacing (move it away from the nearby blue "2") ---
    # tweak offsets as needed; this just nudges the label so it’s readable.
    if state == "Sachsen-Anhalt":
      xy = (centroid.x + 0.20, centroid.y + 0.05)

    # --- CHANGE C: Berlin bubble smaller so it fits inside Berlin ---
    if state == "Berlin":
      fs = 14
      pad = 0.18

    ax.annotate(
      text=str(count),
      xy=xy,
      ha="center",
      va="center",
      fontsize=fs,
      color="black",
      weight="bold",
      zorder=10,
      bbox=dict(boxstyle=f"circle,pad={pad}", fc="white", ec="black", lw=0.5, alpha=1.0),
    )

  plt.tight_layout()
  plot_name = Path(__file__).stem + ".svg"
  plt.savefig(output_dir / plot_name, format="svg", transparent=True)
  plt.close()


def download_file_if_needed(url: str, download_dir: Path, filename: str) -> Path:
  file_path = download_dir / filename
  if not file_path.exists():
    try:
      response = requests.get(url)
      response.raise_for_status()
      file_path.write_bytes(response.content)
    except Exception as e:
      print(f"Failed to download {filename}: {e}")
  return file_path


def load_csv_from_zip(zip_path, target_filename):
  with zipfile.ZipFile(zip_path, "r") as z:
    for name in z.namelist():
      if name.endswith(target_filename):
        with z.open(name) as f:
          return pd.read_csv(f)
  return None


def load_geojson_with_lan_name(geojson_file: Path) -> gpd.GeoDataFrame:
  """
  Loads GeoJSON and extracts the German state name (lan_name) from properties.
  """
  gdf = gpd.read_file(geojson_file)
  with open(geojson_file) as f:
    data = json.load(f)
  lan_names = []
  for feat in data["features"]:
    lan_name = feat["properties"].get("lan_name")
    if isinstance(lan_name, list):
      lan_name = lan_name[0]
    lan_names.append(lan_name)
  gdf["lan_name"] = lan_names
  return gdf


def add_state_counts_to_gdf(gdf: gpd.GeoDataFrame, state_counts: pd.Series) -> gpd.GeoDataFrame:
  counts_df = state_counts.rename("site_count").reset_index()
  counts_df.columns = ["lan_name", "site_count"]
  gdf = gdf.merge(counts_df, on="lan_name", how="left")
  gdf["site_count"] = gdf["site_count"].fillna(0).astype(int)
  return gdf


def main():
  csv_file = get_base_csv_file()
  downloads_dir = get_downloads_dir()
  output_dir = get_output_dir()
  state_counts, nodes_df = prepare_data(csv_file, downloads_dir)
  geojson_file = download_file_if_needed(GEOJSON_URL, downloads_dir, GEOJSON_FILENAME)
  gdf = load_geojson_with_lan_name(geojson_file)
  gdf = add_state_counts_to_gdf(gdf, state_counts)
  plot_network_map(gdf, nodes_df, output_dir)


if __name__ == "__main__":
  main()
