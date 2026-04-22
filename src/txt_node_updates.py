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
@VERSION=1.2.1
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


def first_non_null_from_columns(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
  """
  Returns the first non-null value from the candidate columns that actually exist in the DataFrame.
  If none of the columns exist, a Series with None values is returned.
  """
  existing = [col for col in candidates if col in df.columns]
  if not existing:
    return pd.Series([None] * len(df), index=df.index)
  return df[existing].bfill(axis=1).iloc[:, 0]


def postprocess_updates_df(updates_df: pd.DataFrame) -> pd.DataFrame:
  """
  Combines raw transition columns into to1.5.1, to1.6 and to1.7. Keeps only final columns.
  """
  df = updates_df.copy()
  df["to1.5.1"] = first_non_null_from_columns(
      df,
      [
        "dwh-j2ee-1.5rc1 --> dwh-j2ee-1.5.1rc1",
        "ear-1.5rc1 --> ear-1.5.1rc1",
        "NEW --> dwh-j2ee-1.5.1rc1",
        "NEW --> ear-1.5.1rc1",
      ],
  )
  df["to1.6"] = first_non_null_from_columns(
      df,
      [
        "dwh-j2ee-1.5.1rc1 --> dwh-j2ee-1.6rc1",
        "ear-1.5.1rc1 --> ear-1.6rc1",
        "NEW --> dwh-j2ee-1.6rc1",
        "NEW --> ear-1.6rc1",
      ],
  )
  df["to1.7"] = first_non_null_from_columns(
      df,
      [
        "dwh-j2ee-1.6rc1 --> dwh-j2ee-1.7rc1",
        "ear-1.6rc1 --> ear-1.7rc1",
        "dwh-j2ee-1.5.1rc1 --> dwh-j2ee-1.7rc1",
        "ear-1.5.1rc1 --> ear-1.7rc1",
        "NEW --> dwh-j2ee-1.7rc1",
        "NEW --> ear-1.7rc1",
      ],
  )

  # Special case for clinic 47: use "1.4 --> DELETED" as to1.6
  cond = df["node_id"].astype(str) == "47"
  if "dwh-j2ee-1.4 --> DELETED" in df.columns:
    df.loc[cond, "to1.6"] = df.loc[cond, "dwh-j2ee-1.4 --> DELETED"]
  return df[["node_id", "to1.5.1", "to1.6", "to1.7"]].copy()


def parse_versions_json(file_path: Path) -> str | None:
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


def get_release_date(releases_df: pd.DataFrame, version: str) -> pd.Timestamp:
  """
  Returns the release date for a given j2ee version from the releases CSV.
  """
  j2ee = releases_df[releases_df["type"] == "j2ee"]
  matches = j2ee.loc[j2ee["version"] == version, "release_date"]

  if matches.empty:
    raise ValueError(f"Release date for version '{version}' not found in releases CSV.")

  return pd.to_datetime(matches.iloc[0])


def add_days_to_releases(merged_df: pd.DataFrame, releases_csv: Path) -> pd.DataFrame:
  """
  Adds daysTo1.5.1, daysTo1.6 and daysTo1.7 by calculating the absolute difference in days between each update
  timestamp and the official release date.
  """
  releases_df = pd.read_csv(releases_csv)
  rel_1_5_1_date = get_release_date(releases_df, "v1.5.1rc1")
  rel_1_6_date = get_release_date(releases_df, "v1.6rc1")
  rel_1_7_date = get_release_date(releases_df, "v1.7rc1")
  df = merged_df.copy()

  df["to1.5.1"] = pd.to_datetime(df["to1.5.1"], errors="coerce")
  df["to1.6"] = pd.to_datetime(df["to1.6"], errors="coerce")
  df["to1.7"] = pd.to_datetime(df["to1.7"], errors="coerce")

  df["daysTo1.5.1"] = (rel_1_5_1_date - df["to1.5.1"]).abs().dt.days
  df["daysTo1.6"] = (rel_1_6_date - df["to1.6"]).abs().dt.days
  df["daysTo1.7"] = (rel_1_7_date - df["to1.7"]).abs().dt.days
  return df


def append_timing_summary(lines: list[str], series: pd.Series, label: str) -> None:
  """
  Appends mean, median and IQR summary for a given update timing series.
  """
  values = series.dropna()
  actual_updated = len(values)
  lines.append("")
  if values.empty:
    lines.append(f"No valid data for {label}.")
    return
  mean = values.mean()
  median = values.median()
  q1 = values.quantile(0.25)
  q3 = values.quantile(0.75)
  lines.append(f"Update timing for {label} of {actual_updated} nodes:")
  lines.append(f"Mean days: {mean:.2f}")
  lines.append(f"Median days: {median:.2f}")
  lines.append(f"Q1: {q1:.2f}")
  lines.append(f"Q3: {q3:.2f}")


def summarize_j2ee_updates(df: pd.DataFrame, output_dir: Path) -> None:
  """
  Calculates absolute and percentage distribution per version, keeping missing current
  versions as 'unknown', computes summary statistics for update timing, and saves the
  results to a text file.
  """

  def normalize_version(val):
    if pd.isna(val):
      return "unknown"
    if "1.5.1" in val:
      return "1.5.1"
    if "1.6" in val:
      return "1.6"
    if "1.7" in val:
      return "1.7"
    raise ValueError(f"Unexpected current version value: {val}")

  df_clean = df.copy()
  df_clean["version_group"] = df_clean["current"].apply(normalize_version)
  version_order = ["1.5.1", "1.6", "1.7", "unknown"]
  counts = df_clean["version_group"].value_counts().reindex(version_order, fill_value=0)
  total = counts.sum()
  percentages = (counts / total * 100).round(2)
  lines = ["Nodes per version:"]
  for version in version_order:
    lines.append(f"{version}: {counts[version]} nodes ({percentages[version]}%)")
  append_timing_summary(lines, df_clean["daysTo1.6"], "1.6")
  append_timing_summary(lines, df_clean["daysTo1.7"], "1.7")

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
