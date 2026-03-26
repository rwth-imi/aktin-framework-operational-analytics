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
Created on 7/4/25
@AUTHOR: Alexander Kombeiz (akombeiz@ukaachen.de)
@VERSION=1.2
"""

import zipfile
from pathlib import Path

import pandas as pd

from helper.download_and_aggregate_broker_results import get_or_export_request_result
from helper.paths import get_output_dir, get_downloads_dir

REQUEST_IDS = ["3047", "3077", "3086", "3100", "3105", "3114", "3191", "3233", "3299"]


def create_daily_encounter_df(zip_file: Path) -> pd.DataFrame:
  """
  Opens the main export zip file. For each *_result.zip inside, extracts result.txt, keeps relevant columns, and adds
  the node_id. Combines all sites into one DataFrame.
  """
  results = []
  with zipfile.ZipFile(zip_file, "r") as parent_zip:
    for name in parent_zip.namelist():
      if name.endswith("_result.zip"):
        node_id = name.split("_")[0]

        # Read the inner zip and extract result.txt
        with parent_zip.open(name) as inner_zip_file:
          with zipfile.ZipFile(inner_zip_file) as inner_zip:
            with inner_zip.open("result.txt") as result_file:
              df = pd.read_csv(result_file, sep="\t")

              # Keep only relevant columns and rename for clarity
              df = df[["date", "eingegangene_faelle", "p21_fall"]].copy()
              df = df.rename(columns={"eingegangene_faelle": "encounter", "p21_fall": "p21"})
              df["node_id"] = node_id
              results.append(df)
  combined_df = pd.concat(results, ignore_index=True)
  return combined_df


def get_or_create_daily_encounter_df(downloads_dir: Path) -> pd.DataFrame:
  """
  Loads the daily encounter DataFrame from cache if available.
  Otherwise creates it from the export zip and saves it for reuse.
  """
  cache_file = downloads_dir / "daily_encounter.csv"
  if cache_file.exists():
    print(f"Loading cached DataFrame from {cache_file}")
    df = pd.read_csv(cache_file)
  else:
    print("Cache not found, creating daily encounter DataFrame...")
    zip_file = get_or_export_request_result(downloads_dir, REQUEST_IDS)
    df = create_daily_encounter_df(zip_file)
    df.to_csv(cache_file, index=False)
    print(f"Saved daily encounter DataFrame to {cache_file}")
  return df


def aggregate_daily_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
  """
  Aggregates the daily encounters to a monthly level. Calculates total encounters, total p21, and the number of days
  with encounters. Also computes days without encounters for each month.
  """
  df = df.copy()
  df["date"] = pd.to_datetime(df["date"])
  df["month"] = df["date"].dt.to_period("M")
  df["day"] = df["date"].dt.day
  print("Latest date observed date:", df[df["date"].dt.year == pd.Timestamp.now().year]["date"].max())

  grouped = df.groupby(["node_id", "month"])
  agg_df = grouped.agg(
    encounter=("encounter", "sum"), p21=("p21", "sum"), days_with_encounter=("encounter", lambda x: (x > 0).sum())
  ).reset_index()

  agg_df["days_in_month"] = agg_df["month"].dt.to_timestamp().dt.days_in_month
  agg_df["days_without_encounter"] = agg_df["days_in_month"] - agg_df["days_with_encounter"]

  # Drop intermediate columns
  agg_df = agg_df.drop(columns=["days_in_month"])
  return agg_df


def summarize_yearly_encounter(df: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
  """
  Summarizes the monthly encounter DataFrame to a yearly level. Drops months with too few days with encounters.
  Computes mean, median, IQR for encounter and p21, and counts observed months. Saves the summary as a CSV to output_dir.
  """
  df = df.copy()

  # Remove months with too little data
  df = df[df["days_with_encounter"] > 5]

  df["year"] = df["month"].dt.year
  grouped = df.groupby(["node_id", "year"])
  summary = grouped.agg(
    observed_months=("month", pd.Series.nunique),
    encounter_mean=("encounter", "mean"),
    encounter_std=("encounter", "std"),
    encounter_median=("encounter", "median"),
    encounter_q1=("encounter", lambda x: x.quantile(0.25)),
    encounter_q3=("encounter", lambda x: x.quantile(0.75)),
    encounter_iqr=("encounter", lambda x: x.quantile(0.75) - x.quantile(0.25)),
    p21_mean=("p21", "mean"),
    p21_median=("p21", "median"),
    p21_q1=("p21", lambda x: x.quantile(0.25)),
    p21_q3=("p21", lambda x: x.quantile(0.75)),
    p21_iqr=("p21", lambda x: x.quantile(0.75) - x.quantile(0.25)),
  ).reset_index()

  # Round fraction columns to 2 decimals
  fraction_cols = [
    "encounter_mean",
    "encounter_std",
    "encounter_median",
    "encounter_q1",
    "encounter_q3",
    "encounter_iqr",
    "p21_mean",
    "p21_median",
    "p21_q1",
    "p21_q3",
    "p21_iqr",
  ]
  summary[fraction_cols] = summary[fraction_cols].round(2)

  # Renumber node_id
  # unique_nodes = sorted(summary["node_id"].unique())
  # mapping = {old: new for new, old in enumerate(unique_nodes, start=1)}
  # summary["node_id"] = summary["node_id"].map(mapping)

  table_name = Path(__file__).stem + ".csv"
  summary.to_csv(output_dir / table_name, index=False)
  return summary


def summarize_yearly_overall(df: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
  """
  Summarizes the monthly encounter DataFrame to a yearly level, overall across all nodes,
  and writes a publication-style formatted summary table.
  """
  df = df.copy()

  # Remove months with too little data
  df = df[df["days_with_encounter"] > 5]

  # Compute total days in each month
  df["total_days_in_month"] = df["days_with_encounter"] + df["days_without_encounter"]

  # Compute % of observed days for each node-month row (kept for reference; yearly uses totals below)
  df["observed_days_pct"] = (df["days_with_encounter"] / df["total_days_in_month"]) * 100

  df["year"] = df["month"].dt.year
  summary = []
  for year, group in df.groupby("year"):
    nodes_reporting = group["node_id"].nunique()

    # Observed days totals
    observed_days_total = int(group["days_with_encounter"].sum())
    days_total = int(group["total_days_in_month"].sum())
    observed_days_pct = (observed_days_total / days_total) * 100 if days_total else 0

    # Encounter totals + P21 totals
    total_encounters = float(group["encounter"].sum())
    encounters_with_p21_total = float(group.loc[group["p21"] > 0, "p21"].sum())
    percent_encounters_with_p21 = (encounters_with_p21_total / total_encounters) * 100 if total_encounters else 0

    # Annual totals per node
    node_yearly_totals = group.groupby("node_id")["encounter"].sum()

    # Compute yearly encounter stats across nodes
    encounter_mean = node_yearly_totals.mean()
    encounter_std = node_yearly_totals.std()
    encounter_median = node_yearly_totals.median()
    encounter_q1 = node_yearly_totals.quantile(0.25)
    encounter_q3 = node_yearly_totals.quantile(0.75)
    encounter_iqr = encounter_q3 - encounter_q1

    summary.append(
      {
        "Year": int(year),
        "Reporting Nodes": int(nodes_reporting),
        "Observed Days": f"{observed_days_pct:.2f} (n={observed_days_total})",
        "Total ED Encounters": f"{int(total_encounters):,}",
        "Mean ± SD": (
          f"{round(encounter_mean):,} ± {round(encounter_std):,}"
          if pd.notna(encounter_std)
          else f"{round(encounter_mean):,} ± NA"
        ),
        "Median [Q1 – Q3]": f"{round(encounter_median):,} [{round(encounter_q1):,} – {round(encounter_q3):,}]",
        "IQR": f"{round(encounter_iqr):,}",
        "Inpatient outcomes linked": f"{percent_encounters_with_p21:.2f} (n={int(encounters_with_p21_total):,})",
      }
    )
  summary_df = pd.DataFrame(summary)

  table_name = Path(__file__).stem + "2.csv"
  summary_df.to_csv(output_dir / table_name, index=False)
  return summary_df


def main():
  downloads_dir = get_downloads_dir()
  output_dir = get_output_dir()
  daily_df = get_or_create_daily_encounter_df(downloads_dir)
  monthly_df = aggregate_daily_to_monthly(daily_df)
  summarize_yearly_encounter(monthly_df, output_dir)
  summarize_yearly_overall(monthly_df, output_dir)


if __name__ == "__main__":
  main()
