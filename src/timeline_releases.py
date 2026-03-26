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
Created on 7/8/25
@AUTHOR: Alexander Kombeiz (akombeiz@ukaachen.de)
@VERSION=2.0
"""

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from helper.paths import get_modified_releases_csv_file, get_output_dir

TYPE_COLORS = {
  "docker": "purple",
  "deb": "red",
  "j2ee": "blue",
  "broker": "green"
}

DISPLAY_NAMES = {
  "docker": "DWH Docker",
  "deb": "DWH Debian Package",
  "j2ee": "DWH EAR",
  "broker": "Broker"
}

LANE_POSITIONS = {
  "docker": 4,
  "deb": 3,
  "j2ee": 2,
  "broker": 1
}

PATCH_OFFSET = 0.125
STAGGER_MAIN_OFFSET = 0.5


def load_and_clean_csv(csv_file: Path) -> pd.DataFrame:
  """
  Load the CSV file, parse release dates, and drop rows with invalid dates.
  """
  df = pd.read_csv(csv_file)
  df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
  df = df.dropna(subset=["release_date"])
  return df


def process_sub_df(df: pd.DataFrame) -> pd.DataFrame:
  """
  Processes a single release type DataFrame by setting base heights for MAJOR and MINOR releases, filling PATCH heights
  based on the nearest parent release, and linking each PATCH to its parent with 'patch_for'.
  """
  df["height"] = df["release_type"].map({"MAJOR": 4, "MINOR": 2})
  df["patch_for"] = pd.NaT
  df = df.sort_values("release_date", ascending=False).reset_index(drop=True)
  heights = df["height"].copy()
  patch_for = df["patch_for"].copy()
  for idx, row in df.iterrows():
    if pd.isna(row["height"]):
      for next_pos in range(idx + 1, len(df)):
        next_row = df.iloc[next_pos]
        if not pd.isna(next_row["height"]):
          heights.iloc[idx] = next_row["height"]
          patch_for.iloc[idx] = next_row["release_date"]
          break
  df["height"] = heights
  df["patch_for"] = patch_for
  return df


def create_plot_df(csv_file: Path) -> pd.DataFrame:
  df = load_and_clean_csv(csv_file)
  processed_parts = []
  for t in DISPLAY_NAMES.keys():
    sub_df = df[df["type"] == t].copy()
    if not sub_df.empty:
      processed = process_sub_df(sub_df)
      processed_parts.append(processed)
  final = pd.concat(processed_parts, ignore_index=True)
  final["color"] = final["type"].map(TYPE_COLORS)
  final["y_pos"] = final["type"].map(LANE_POSITIONS)
  return final


def plot_release_timeline(df: pd.DataFrame, output_dir: Path):
  fig, ax = plt.subplots(figsize=(16, 6))
  df = df.sort_values(by=["type", "release_date"])

  # Manual selection for Broker versions that need to be shifted down
  versions_to_stagger = ["1.2.0", "1.4.0"]
  # Track dates of these versions so their patches follow suit
  staggered_parent_dates = set()
  # Pass 1: Identify Parent Dates to Stagger
  for _, row in df.iterrows():
    if row["type"] == "broker" and row["version"] in versions_to_stagger:
      staggered_parent_dates.add(row["release_date"])

  # -------------------------------------------------------------
  # 2. Main Plotting Loop
  # -------------------------------------------------------------
  for _, row in df.iterrows():
    x = row["release_date"]
    current_type = row["type"]
    y_lane_base = LANE_POSITIONS.get(current_type, 0)
    color = row["color"]
    version = row["version"]
    is_patch = (row["release_type"] == "PATCH")
    is_staggered_group = False
    if current_type == "broker":
      if not is_patch:
        if row["release_date"] in staggered_parent_dates:
          is_staggered_group = True
      else:
        if not pd.isna(row["patch_for"]) and row["patch_for"] in staggered_parent_dates:
          is_staggered_group = True

    y_plot = y_lane_base

    # 1. Apply Stagger Offset (Shift whole group down)
    if is_staggered_group:
      y_plot -= STAGGER_MAIN_OFFSET

    # 2. Apply Patch Offset (Shift patch relative to its group baseline)
    # The baseline is either the normal lane or the staggered lane
    group_baseline = y_plot

    if is_patch:
      y_plot -= PATCH_OFFSET

    # Connector for Patch Lines
    if is_patch and not pd.isna(row["patch_for"]):
      patch_start = row["patch_for"]

      # Horizontal dashed line (The "patch track")
      ax.hlines(y_plot, patch_start, x, color=color, lw=2, linestyle="--")

      # Vertical connector for the patch track
      # Connects up to the CURRENT Group Baseline (which might be staggered or normal)
      # This ensures the patch line connects to where the parent dot actually is
      ax.vlines(patch_start, y_plot, group_baseline, color=color, lw=2, linestyle="--")

    # --- Plot Marker ---
    ax.plot(x, y_plot, marker='o', markersize=6, color=color)

    # --- Annotation Styling ---
    if is_patch:
      xytext = (0, -15)
      bbox_props = None
    else:
      xytext = (0, 10)
      bbox_props = dict(boxstyle="round,pad=0.2", fc="white", alpha=0.7, ec=color, lw=1.5)
    ax.annotate(
        version,
        xy=(x, y_plot),
        xytext=xytext,
        textcoords="offset points",
        bbox=bbox_props,
        fontsize=12,
        ha="center",
        color="black"
    )
  start = df["release_date"].min()
  end = df["release_date"].max()

  # Baseline
  ax.hlines(y=0, xmin=pd.Timestamp(f"{start.year}-01-01"), xmax=end, color="black", lw=2)
  ax.annotate(
      "", xy=(end + pd.Timedelta(days=60), 0), xytext=(end, 0),
      arrowprops=dict(arrowstyle="->", color="black", lw=1.5)
  )

  # Limits & Axis
  ax.set_ylim(-1, 5.5)
  ax.yaxis.set_visible(False)
  ax.xaxis.set_visible(False)

  # Year Ticks
  years = pd.date_range(start=f"{start.year}-01-01", end=f"{end.year}-01-01", freq="YS")
  for year in years:
    ax.vlines(year, -0.1, 0.1, color="black", lw=1)
    ax.annotate(
        str(year.year), xy=(year, 0),
        xytext=(0, -15), textcoords="offset points",
        ha="center", va="top", fontsize=14, fontweight='bold'
    )

  # Labels
  min_date = df["release_date"].min()
  for type_key, y_pos in LANE_POSITIONS.items():
    if type_key in DISPLAY_NAMES and type_key in TYPE_COLORS:
      ax.text(
          min_date - pd.Timedelta(days=160),
          y_pos,
          DISPLAY_NAMES[type_key],
          color=TYPE_COLORS[type_key],
          fontsize=14,
          fontweight='bold',
          ha='right',
          va='center'
      )

  plt.tight_layout()
  plot_name = Path(__file__).stem + "_swimlane.svg"
  plt.savefig(output_dir / plot_name, format="svg", transparent=True)
  plt.close()


def main():
  csv_file = get_modified_releases_csv_file()
  output_dir = get_output_dir()
  df = create_plot_df(csv_file)
  plot_release_timeline(df, output_dir)


if __name__ == "__main__":
  main()
