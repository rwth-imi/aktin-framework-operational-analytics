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
@VERSION=1.1
"""

from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import seaborn as sns

from helper.paths import get_base_csv_file, get_output_dir


def parse_date(date: str) -> pd.Timestamp | None:
  """
  Convert date string to Timestamp. Supports 'MM-YYYY' and 'DD-MM-YYYY'. Returns None if unknown or unparsable.
  """
  if date == "???":
    return None
  for fmt in ("%m-%Y", "%d-%m-%Y"):
    try:
      parsed = pd.to_datetime(date, format=fmt)
      if not pd.isna(parsed):
        return parsed
    except (ValueError, TypeError):
      continue
  return None


def create_cumulative_counts_dataframe(csv_file: Path) -> pd.DataFrame:
  """
  Loads a CSV file, parses date columns, filters valid dates, calculates monthly cumulative counts for 'data_since' and
  daily cumulative counts for 'monitored_since', and returns a single DataFrame with 'date', 'Cumulative_EDs', and
  'Type' columns for plotting.
  """
  df = pd.read_csv(csv_file)

  # Parse Monitored Data (Real data 2022+)
  df["monitored_since"] = df["monitored_since"].apply(parse_date)
  monitored_since = df.dropna(subset=["monitored_since"])
  monitored_counts = (
    monitored_since.groupby(monitored_since["monitored_since"].dt.date)
    .size()
    .sort_index()
    .cumsum()
  )
  monitored_counts.index = pd.to_datetime(monitored_counts.index)
  real_df = pd.DataFrame({
    "date": monitored_counts.index,
    "Cumulative_EDs": monitored_counts.values
  })

  # Manual Interpolation
  manual_data = {
    "2017-01-01": 12,
    "2018-01-01": 16,
    "2019-01-01": 17,
    "2020-01-01": 21,
    "2021-01-01": 46,
    "2022-01-01": 50,
  }
  manual_df = pd.DataFrame({
    "date": pd.to_datetime(list(manual_data.keys())),
    "Cumulative_EDs": list(manual_data.values())
  })
  final_df = pd.concat([manual_df, real_df]).sort_values("date").reset_index(drop=True)
  return final_df


def plot_cumulative_ed_trends(df_plot: pd.DataFrame, output_dir: Path):
  sns.set_style("whitegrid")
  fig, ax = plt.subplots(figsize=(6, 6))
  sns.lineplot(
      data=df_plot,
      x="date",
      y="Cumulative_EDs",
      ax=ax,
      linewidth=2,
      color="#2c7bb6"
  )

  # Calculate and Plot Dots for Start of Each Year
  # Resample to Year Start ('YS') and use forward fill to find the cumulative count at that date
  min_year = df_plot["date"].dt.year.min()
  max_year = df_plot["date"].dt.year.max() + 1
  year_starts = [pd.Timestamp(f"{y}-01-01") for y in range(min_year, max_year)]
  x_nums = mdates.date2num(df_plot["date"])
  target_nums = mdates.date2num(year_starts)
  y_values = df_plot["Cumulative_EDs"].values
  interpolated_y = np.interp(target_nums, x_nums, y_values)
  valid_mask = (target_nums >= x_nums.min()) & (target_nums <= x_nums.max())
  ax.scatter(
      x=np.array(year_starts)[valid_mask],
      y=interpolated_y[valid_mask],
      color="#2c7bb6",
      s=60,
      zorder=5,
      edgecolor='white',
      linewidth=0.5
  )

  # Axes Configuration
  ax.set_ylabel("Connected DWH nodes", fontsize=14)
  ax.set_xlabel("Year", fontsize=14)
  ax.grid(True, axis='y')
  ax.xaxis.grid(False)
  ax.xaxis.set_major_locator(mdates.YearLocator())
  ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
  ax.tick_params(axis="x", which="major", pad=10, rotation=0, labelsize=12)
  ax.tick_params(axis="y", labelsize=12)
  ax.yaxis.set_major_locator(ticker.MultipleLocator(5))
  ax.set_ylim(0, df_plot["Cumulative_EDs"].max() + 5)

  # Vertical Line
  start_date = pd.to_datetime("2022-01-28")
  ax.vlines(start_date, 0, df_plot["Cumulative_EDs"].max() * 0.75, color="#ff7f0e", lw=2, linestyle="--")
  ax.annotate(
      "Start of Monitoring",
      xy=(start_date, df_plot["Cumulative_EDs"].max() * 0.75),
      xytext=(0, 5),
      textcoords="offset points",
      bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#ff7f0e", lw=1.5),
      fontsize=12,
      ha="center",
      color="#333333"
  )
  plt.tight_layout()
  plot_name = Path(__file__).stem + ".svg"
  plt.savefig(output_dir / plot_name, format="svg", transparent=True)
  plt.close()


def main():
  csv_file = get_base_csv_file()
  output_dir = get_output_dir()
  df_plot = create_cumulative_counts_dataframe(csv_file)
  plot_cumulative_ed_trends(df_plot, output_dir)


if __name__ == "__main__":
  main()
