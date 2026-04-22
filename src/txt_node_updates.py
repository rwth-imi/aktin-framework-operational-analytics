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
Created on 7/10/25
@AUTHOR: Alexander Kombeiz (akombeiz@ukaachen.de)
@VERSION=1.1
"""

import json
from pathlib import Path
from typing import List

import pandas as pd

from helper.paths import get_base_csv_file, get_downloads_dir, get_output_dir, get_releases_csv_file, get_derived_dir

# log entries after the cutoff are ignored
CUTOFF_DATE = "2026-04-01"


def extract_node_id_from_path(file_path: Path) -> str:
  return file_path.parent.name


def create_monitoring_start_df(base_csv: Path) -> pd.DataFrame:
  """
  Loads the base CSV and returns a DataFrame with node ID as string and the monitored_since date.
  """
  df = pd.read_csv(base_csv)
  df["node_id"] = df["node"].astype(str)
  return df[["node_id", "monitored_since"]].copy()


def find_files(downloads_dir: Path, keyword: str, suffix: str) -> List[Path]:
  """
  Searches all node directories in the downloads folder and returns a list of file paths with given keyword.
  """
  return sorted(
      file
      for node_dir in downloads_dir.iterdir()
      if node_dir.is_dir()
      for file in node_dir.iterdir()
      if file.is_file() and keyword in file.name and file.suffix == suffix
  )


def find_all_version_logs(downloads_dir: Path) -> List[Path]:
  return find_files(downloads_dir, "log_versions", ".log")


def find_all_version_files(downloads_dir: Path) -> List[Path]:
  return find_files(downloads_dir, "versions", ".txt")


def create_updates_df(downloads_dir: Path) -> pd.DataFrame:
  """
  Reads all version log files, extracts version transitions for dwh-j2ee or ear, and returns a DataFrame with node ID
  and timestamps.
  """
  rows = []
  cutoff = pd.to_datetime(CUTOFF_DATE, format="%Y-%m-%d").date()
  for file_path in find_all_version_logs(downloads_dir):
    node_id = extract_node_id_from_path(file_path)
    entry = {"node_id": node_id}
    with file_path.open(encoding="utf-8") as f:
      for line in f:
        if "dwh-j2ee" in line or "ear" in line:
          parts = line.strip().split(" : ")
          if len(parts) >= 2:
            timestamp_str = parts[0]
            timestamp = pd.to_datetime(timestamp_str, utc=True, errors="coerce")
            if pd.isna(timestamp):
              continue
            if timestamp.date() > cutoff:
              continue
            transition = parts[1].split("]")[-1].strip()
            entry[transition] = timestamp.date().isoformat()
    rows.append(entry)
  return pd.DataFrame(rows)


def postprocess_updates_df(updates_df: pd.DataFrame) -> pd.DataFrame:
  """
  Combines raw transition columns into to1.5.1 and to1.6. Keeps only final columns.
  """
  df = updates_df.copy()
  # Create target columns with fallback: first non-null value wins
  df["to1.5.1"] = df[["dwh-j2ee-1.5rc1 --> dwh-j2ee-1.5.1rc1", "NEW --> dwh-j2ee-1.5.1rc1"]].bfill(axis=1).iloc[:, 0]
  df["to1.6"] = df[["dwh-j2ee-1.5.1rc1 --> dwh-j2ee-1.6rc1", "NEW --> dwh-j2ee-1.6rc1"]].bfill(axis=1).iloc[:, 0]
  # Special case for clinic 47: use "1.4 --> DELETED" as to1.6
  cond = df["node_id"].astype(str) == "47"
  df.loc[cond, "to1.6"] = df.loc[cond, "dwh-j2ee-1.4 --> DELETED"]
  return df[["node_id", "to1.5.1", "to1.6"]].copy()


def parse_versions_json(file_path: Path) -> str:
  """
  Loads a JSON versions file and returns the installed version, prioritizing dwh-j2ee and falling back to ear.
  """
  with file_path.open(encoding="utf-8") as vf:
    try:
      versions_json = json.load(vf)
      return versions_json.get("dwh-j2ee") or versions_json.get("ear")
    except Exception:
      return None


def create_versions_df(downloads_dir: Path) -> pd.DataFrame:
  """
  Loads all current versions files and returns a DataFrame with node ID and installed version.
  """
  rows = []
  for file_path in find_all_version_files(downloads_dir):
    node_id = extract_node_id_from_path(file_path)
    rows.append({"node_id": node_id, "current": parse_versions_json(file_path)})
  return pd.DataFrame(rows)


def merge_node_data(monitoring_df: pd.DataFrame, versions_df: pd.DataFrame, updates_df: pd.DataFrame) -> pd.DataFrame:
  """
  Merges monitoring, versions and updates DataFrames on node ID and returns the merged, sorted result.
  """
  df = monitoring_df.merge(versions_df, on="node_id", how="outer")
  df = df.merge(updates_df, on="node_id", how="outer")
  df["node_id"] = df["node_id"].astype(int)
  return df.sort_values("node_id").reset_index(drop=True)


def add_days_to_releases(merged_df: pd.DataFrame, releases_csv: Path) -> pd.DataFrame:
  """
  Adds daysTo1.5.1 and daysTo1.6 by calculating the absolute difference in days between each update timestamp and the
  official release date.
  """
  releases_df = pd.read_csv(releases_csv)
  j2ee = releases_df[releases_df["type"] == "j2ee"]
  rel_1_5_1_date = pd.to_datetime(j2ee[j2ee["version"] == "v1.5.1rc1"]["release_date"].iloc[0])
  rel_1_6_date = pd.to_datetime(j2ee[j2ee["version"] == "v1.6rc1"]["release_date"].iloc[0])
  df = merged_df.copy()
  # Ensure update timestamps are datetime
  df["to1.5.1"] = pd.to_datetime(df["to1.5.1"], errors="coerce")
  df["to1.6"] = pd.to_datetime(df["to1.6"], errors="coerce")
  # Compute absolute differences
  df["daysTo1.5.1"] = (rel_1_5_1_date - df["to1.5.1"]).abs().dt.days
  df["daysTo1.6"] = (rel_1_6_date - df["to1.6"]).abs().dt.days
  return df


def summarize_j2ee_updates(df: pd.DataFrame, output_dir: Path) -> None:
  """
  Adds nodes with empty 'current' to the version 1.6 count. Calculates absolute and percentage distribution
  per version, counts how many nodes actually updated to 1.6 based on 'daysTo1.6', computes mean, median, and IQR
  for update timing, and saves the results to 'update_summary.txt' in the output directory.
  """

  def normalize_version(val):
    if pd.isna(val):
      return "other"
    if "1.5.1" in val:
      return "1.5.1"
    if "1.6" in val:
      return "1.6"
    return "other"

  no_current = sum(df["current"].notna() == False)
  df_clean = df[df["current"].notna()].copy()
  df_clean["version_group"] = df_clean["current"].apply(normalize_version)

  # Count & percentage
  counts = df_clean["version_group"].value_counts().sort_index()
  total = counts.sum()
  percentages = (counts / total * 100).round(2)
  lines = ["Nodes per version:"]
  for version in counts.index:
    if version == "1.6":
      counts[version] += no_current
    lines.append(f"{version}: {counts[version]} nodes ({percentages[version]}%)")

  # Updates to 1.6
  days_to_1_6 = df_clean["daysTo1.6"].dropna()
  actual_updated = len(days_to_1_6)
  # Mean, median, IQR for daysTo1.6
  if not days_to_1_6.empty:
    mean = days_to_1_6.mean()
    median = days_to_1_6.median()
    q1 = days_to_1_6.quantile(0.25)
    q3 = days_to_1_6.quantile(0.75)
    lines.append("")
    lines.append(f"Update timing to 1.6 of {actual_updated} nodes:")
    lines.append(f"Mean days: {mean:.2f}")
    lines.append(f"Median days: {median:.2f}")
    lines.append(f"Q1: {q1:.2f}")
    lines.append(f"Q3: {q3:.2f}")
  else:
    lines.append("")
    lines.append("No valid data for daysTo1.6.")
  summary_name = Path(__file__).stem + ".txt"
  output_file = output_dir / summary_name
  output_file.write_text("\n".join(lines), encoding="utf-8")
  derived_file = get_derived_dir() / summary_name
  derived_file.write_text("\n".join(lines), encoding="utf-8")


def main():
  base_csv = get_base_csv_file()
  releases_csv = get_releases_csv_file()
  downloads_dir = get_downloads_dir()
  output_dir = get_output_dir()

  monitoring_df = create_monitoring_start_df(base_csv)
  versions_df = create_versions_df(downloads_dir)
  updates_df = create_updates_df(downloads_dir)
  updates_df = postprocess_updates_df(updates_df)
  merged_df = merge_node_data(monitoring_df, versions_df, updates_df)
  merged_df = add_days_to_releases(merged_df, releases_csv)
  summarize_j2ee_updates(merged_df, output_dir)


if __name__ == "__main__":
  main()
