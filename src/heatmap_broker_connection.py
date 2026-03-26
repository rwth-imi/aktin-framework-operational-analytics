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
@VERSION=1.01
"""

from pathlib import Path
from typing import List

import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

from helper.paths import get_downloads_dir, get_output_dir


def find_all_stats_csv(downloads_dir: Path) -> List[Path]:
  """
  Walk through all subfolders in the downloads directory. Returns a sorted list of Paths for all files matching
  "*_stats_*.csv".
  """
  stats_files = []
  for node_dir in downloads_dir.iterdir():
    if node_dir.is_dir():
      for file in node_dir.iterdir():
        if file.is_file() and "_stats_" in file.name and file.suffix == ".csv":
          stats_files.append(file)
  return sorted(stats_files)


def extract_node_id_from_path(file_path: Path) -> str:
  return file_path.parent.name


def preprocess_df(csv_file: Path) -> pd.DataFrame:
  """
  Loads the CSV, keeps only "date" and "last_contact", parses them as UTC timestamps, drops rows with invalid values,
  and returns the cleaned two-column DataFrame.
  """
  df = pd.read_csv(csv_file, sep=";")
  df = df[["date", "last_contact"]].copy()
  df["date"] = df["date"].apply(lambda x: pd.to_datetime(x, utc=True))
  df["last_contact"] = df["last_contact"].apply(lambda x: pd.to_datetime(x, utc=True))
  df = df.dropna(subset=["date", "last_contact"])
  return df


def calculate_handshake_diff_minutes(df: pd.DataFrame) -> pd.DataFrame:
  """
  Takes a DataFrame with "date" and "last_contact". Calculates the absolute difference in minutes and adds a
  "within_threshold" column which is True if the difference is 15 minutes or less.
  """
  df = df.copy()
  df["diff_minutes"] = (df["date"] - df["last_contact"]).abs().dt.total_seconds() / 60.0
  df["within_threshold"] = df["diff_minutes"] <= 15
  df["within_threshold"] = df["within_threshold"].fillna(False)
  return df


def compute_monthly_threshold_percentage(df: pd.DataFrame, node_id: str) -> pd.DataFrame:
  """
  Takes a DataFrame with "date" and "within_threshold" for one node. Groups by month to calculate the percentage of
  days within threshold, the number of days observed, and the number of unobserved days in each month. Adds the node_id
  as a reference.
  """
  df = df.copy()
  df["date"] = pd.to_datetime(df["date"])
  df["month"] = df["date"].dt.tz_convert(None).dt.to_period("M")
  df["day"] = df["date"].dt.day

  grouped = df.groupby("month").agg(
      days_within_threshold=("within_threshold", "sum"),
      days_observed=("day", pd.Series.nunique)
  )
  grouped["threshold_percentage"] = (grouped["days_within_threshold"] / grouped["days_observed"]) * 100
  grouped["days_in_month"] = grouped.index.to_timestamp().days_in_month
  grouped["unobserved_days"] = grouped["days_in_month"] - grouped["days_observed"]
  grouped["node_id"] = node_id
  result = grouped.reset_index()[["node_id", "month", "threshold_percentage", "days_observed", "unobserved_days"]]
  return result


def collect_all_sites_monthly_percentages(stats_files: list[Path]) -> pd.DataFrame:
  """
  Takes a list of stats CSV file paths, processes each one, and returns a combined DataFrame with node_id, month,
  threshold_percentage, days_observed, and unobserved_days.
  """
  results = []
  for idx, file_path in enumerate(stats_files, start=1):
    node_id = extract_node_id_from_path(file_path)
    df = preprocess_df(file_path)
    df = calculate_handshake_diff_minutes(df)
    result = compute_monthly_threshold_percentage(df, node_id)
    results.append(result)
    percent = round(idx / len(stats_files) * 100, 2)
    print(f"Progress: {percent}% ({idx}/{len(stats_files)})")
  combined_df = pd.concat(results, ignore_index=True)
  return combined_df


def get_or_create_combined_df(downloads_dir: Path) -> pd.DataFrame:
  """
  Checks if a cached combined DataFrame exists in /tmp/. If yes, loads it. If not, computes it, stores it in /tmp/,
  and returns it.
  """
  cache_file = downloads_dir / "combined_connection_df.csv"
  if cache_file.exists():
    print(f"Loading cached DataFrame from {cache_file}")
    combined_df = pd.read_csv(cache_file)
  else:
    print("Cache not found, creating combined DataFrame...")
    stats_files = find_all_stats_csv(downloads_dir)
    combined_df = collect_all_sites_monthly_percentages(stats_files)
    combined_df.to_csv(cache_file, index=False)
    print(f"Saved combined DataFrame to {cache_file}")
  return combined_df


def postprocess_df(df: pd.DataFrame) -> pd.DataFrame:
  """
  Cleans the DataFrame by dropping rows for the months 2022-01 to 2022-04, removing rows where days_observed is less
  than 5, rounding threshold_percentage to a whole number, and sorting by node_id ascending.
  """
  df = df.copy()

  # Drop unwanted months
  months_to_drop = ["2022-01", "2022-02", "2022-03", "2026-01"]
  df = df[~df["month"].astype(str).isin(months_to_drop)]

  # Drop low-observation rows
  df = df[df["days_observed"] >= 5]

  # Round percentages
  df["threshold_percentage"] = df["threshold_percentage"].round()

  # Sort by node_id ascending
  unique_nodes = sorted(df["node_id"].unique())
  mapping = {old: new for new, old in enumerate(unique_nodes, start=1)}
  df["node_id"] = df["node_id"].map(mapping)
  df = df.sort_values("node_id", ascending=True)

  # Renumber node_id
  unique_nodes = sorted(df["node_id"].unique())
  mapping = {old: new for new, old in enumerate(unique_nodes, start=1)}
  df["node_id"] = df["node_id"].map(mapping)

  return df


def plot_connection_heatmap(df: pd.DataFrame, output_dir: Path):
  heatmap_df = df.pivot_table(
      index="node_id",
      columns="month",
      values="threshold_percentage"
  ).sort_index()

  # Build custom annotation DataFrame
  annot_df = heatmap_df.apply(lambda col: col.map(lambda x: f"{x:.0f}%" if pd.notna(x) and 0 < x < 100 else ""))

  # Plot Configuration
  plt.figure(figsize=(20, 24))
  sns.set_style("whitegrid")
  ax = sns.heatmap(
      heatmap_df,
      annot=annot_df,
      fmt="",
      cmap="RdYlGn",
      vmin=0,
      vmax=100,
      linewidths=0.3,
      linecolor="grey",
      cbar=False,
      cbar_kws={"label": "Connection %"},
  )

  # Title Axis labels
  ax.set_xlabel("Month", fontsize=22)
  ax.set_ylabel("AKTIN Data Warehouse Node", fontsize=22)

  # Axis Ticks
  plt.xticks(rotation=55, ha="right", fontsize=16)
  plt.yticks(rotation=0, fontsize=16)

  plt.tight_layout()
  plot_name = Path(__file__).stem + ".svg"
  plt.savefig(output_dir / plot_name, format="svg", transparent=True)
  plt.close()


def compute_yearly_connection_stats(df: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
  """
  For each year, computes the number of unique nodes that were observed, the total percentage
  of observed days based on actual calendar days (using both observed and unobserved day counts),
  the average handshake reliability across all nodes (weighted by observed days), and the number
  of months with available data.
  """
  df = df.copy()
  df["year"] = df["month"].astype(str).str[:4].astype(int)

  # Count how many unique months had data in each year
  months_per_year = df.groupby("year")["month"].nunique().reset_index(name="months_with_data")

  # Aggregate observed/unobserved days and handshake % per node per year
  yearly = df.groupby(["year", "node_id"]).agg(
      observed_days=("days_observed", "sum"),
      unobserved_days=("unobserved_days", "sum"),
      handshake_avg=("threshold_percentage", "mean")
  ).reset_index()

  yearly["total_days"] = yearly["observed_days"] + yearly["unobserved_days"]

  # Convert handshake percentage back into "handshake days within threshold"
  # (days_within_threshold ~= observed_days * handshake_avg/100)
  yearly["handshake_days"] = (yearly["handshake_avg"] / 100.0) * yearly["observed_days"]

  def aggregate_year(group: pd.DataFrame) -> pd.Series:
    total_observed_days = float(group["observed_days"].sum())
    total_days = float(group["total_days"].sum())
    total_handshake_days = float(group["handshake_days"].sum())

    observed_days_pct = (total_observed_days / total_days) * 100 if total_days else 0
    avg_handshake_reliability = (total_handshake_days / total_observed_days) * 100 if total_observed_days else 0

    return pd.Series({
      "observed_nodes": int(group["node_id"].nunique()),
      "total_observed_days": int(round(total_observed_days)),
      "total_days": int(round(total_days)),
      "observed_days_pct": observed_days_pct,
      "handshake_within_threshold_days": int(round(total_handshake_days)),
      "avg_handshake_reliability": avg_handshake_reliability,
    })

  result = yearly.groupby("year", group_keys=False).apply(aggregate_year, include_groups=False).reset_index()
  result = result.merge(months_per_year, on="year")
  result["observed_days_pct"] = result["observed_days_pct"].round(2)
  result["avg_handshake_reliability"] = result["avg_handshake_reliability"].round(2)

  result = result[
    [
      "year",
      "observed_nodes",
      "months_with_data",
      "total_observed_days",
      "total_days",
      "observed_days_pct",
      "handshake_within_threshold_days",
      "avg_handshake_reliability",
    ]
  ]
  table_name = Path(__file__).stem + ".csv"
  result.to_csv(output_dir / table_name, index=False)
  return result


def main():
  downloads_dir = get_downloads_dir()
  output_dir = get_output_dir()
  results = get_or_create_combined_df(downloads_dir)
  results = postprocess_df(results)
  compute_yearly_connection_stats(results, output_dir)
  plot_connection_heatmap(results, output_dir)


if __name__ == "__main__":
  main()
