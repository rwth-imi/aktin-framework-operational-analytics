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

import matplotlib.pyplot as plt
import pandas as pd

from helper.paths import get_base_csv_file, get_output_dir

# nodes with monitored_since after the cutoff are ignored
CUTOFF_DATE = "01-04-2026"


def normalize_edis_name(edis: str) -> str:
  """
  Convert short EDIS name to a cleaner name for plots.
  Unknown names stay capitalized.
  """
  mapping = {
    "erpath": "ERPath",
    "imeso": "IMESO",
    "epias": "EPIAS",
    "ecare": "E.Care",
    "orbis": "ORBIS",
    "copra": "COPRA",
    "ishmed": "i.s.h.med",
    "medico": "Medico",
    "sap": "SAP",
    "mayrhofer": "Mayrhofer",
    "imedone": "iMedOne",
    "meona": "Meona",
    "cerner": "Cerner",
  }
  return mapping.get(edis.lower(), edis.capitalize())


def get_edis_counts(csv_file: Path, cutoff_date: str | None = None) -> pd.Series:
  """
  Load the CSV and count how many sites use each EDIS. Rare systems (<= 2) and unknowns (???) are grouped into "Other".
  """
  df = pd.read_csv(csv_file)

  # Parse monitored_since as day-first dates like 28-01-2022
  df["monitored_since"] = pd.to_datetime(df["monitored_since"], format="%d-%m-%Y", errors="coerce")

  # Apply cutoff if provided
  if cutoff_date is not None:
    cutoff = pd.to_datetime(cutoff_date, format="%d-%m-%Y")
    df = df[df["monitored_since"].notna()]
    df = df[df["monitored_since"] <= cutoff]

  # Normalize names and handle unknowns
  df["edis_clean"] = df["edis"].apply(lambda x: "Unreported" if x == "???" else normalize_edis_name(x))
  counts = df["edis_clean"].value_counts()

  # Group rare systems into Other
  rare = counts[counts <= 2]
  common = counts[counts > 2]
  misc_count = rare.sum()
  if misc_count > 0:
    if "Others" in common:
      common["Others"] += misc_count
    else:
      common["Others"] = misc_count
  return common.sort_values(ascending=False)


def plot_piechart(edis_counts: pd.Series, output_dir: Path):
  fig, ax = plt.subplots(figsize=(10, 10))

  def format_pct(pct):
    total = edis_counts.sum()
    value = int(round(pct / 100.0 * total))
    return f"{pct:.1f}%\n({value})"

  ax.pie(edis_counts, labels=edis_counts.index, autopct=format_pct, startangle=140, textprops={"fontsize": 16})
  plt.tight_layout()
  plot_name = "piechart_edis.svg"
  plt.savefig(output_dir / plot_name, format="svg", transparent=True)
  plt.close()


def plot_barchart(edis_counts: pd.Series, output_dir: Path):
  # Keep normal descending order, but force "Unreported" to the very bottom
  data = edis_counts.copy()
  if "Unreported" in data.index:
    unreported_value = data.pop("Unreported")
    data["Unreported"] = unreported_value

  # Reverse so the largest bar appears at the top in barh,
  # while "Unreported" stays at the very bottom
  data = data.iloc[::-1]

  fig, ax = plt.subplots(figsize=(6, 6))
  colors = ["#2c7bb6"] * len(data)
  if "Unreported" in data.index:
    colors[data.index.get_loc("Unreported")] = "#b0b0b0"

  bars = ax.barh(data.index, data.values, color=colors, alpha=0.9)
  total = data.sum()

  # Annotate bars with "Count (Percentage%)"
  for bar in bars:
    width = bar.get_width()
    pct = (width / total) * 100
    label = f"{int(width)} ({pct:.1f}%)"
    ax.text(width + 0.5, bar.get_y() + bar.get_height() / 2, label, va="center", fontsize=12)

  ax.spines["top"].set_visible(False)
  ax.spines["right"].set_visible(False)
  ax.grid(axis="x", linestyle="--", alpha=0.5)
  ax.set_xlabel("Number of EDs", fontsize=14)
  ax.tick_params(axis="x", which="major", labelsize=12)
  ax.tick_params(axis="y", which="major", labelsize=12)
  plt.tight_layout()

  plot_name = "barchart_edis.svg"
  plt.savefig(output_dir / plot_name, format="svg", bbox_inches="tight")
  plt.close()


def main():
  csv_file = get_base_csv_file()
  output_dir = get_output_dir()
  edis_counts = get_edis_counts(csv_file, CUTOFF_DATE)
  plot_piechart(edis_counts, output_dir)
  plot_barchart(edis_counts, output_dir)


if __name__ == "__main__":
  main()
