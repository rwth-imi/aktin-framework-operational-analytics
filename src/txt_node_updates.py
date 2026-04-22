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
@VERSION=2.0
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


def parse_node_updates(file_path: Path) -> List[dict]:
  node_id = extract_node_id_from_path(file_path)
  parsed_lines = []

  with file_path.open(encoding="utf-8") as f:
    for line in f:
      if " : " not in line:
        continue
      time_str, action_str = line.strip().split(" : ", 1)
      date = pd.to_datetime(time_str, utc=True, errors="coerce").date()
      if pd.isna(date):
        continue
      parsed_lines.append((date, action_str))

  parsed_lines.sort(key=lambda x: x[0])

  actions_by_date = {}
  for date, action_str in parsed_lines:
    if date not in actions_by_date:
      actions_by_date[date] = []
    actions_by_date[date].append(action_str)

  apache2_installed = True
  postgres_installed = True
  updates = []

  for date, actions in actions_by_date.items():
    from_state = "Docker" if not apache2_installed and not postgres_installed else "Debian"

    app_updates_today = []

    for action_str in actions:
      if "[apache2]" in action_str:
        apache2_installed = "[not installed]" not in action_str.split("-->")[1]
      if "[postgres]" in action_str:
        postgres_installed = "[not installed]" not in action_str.split("-->")[1]

      if "[dwh-j2ee]" in action_str or "[ear]" in action_str:
        transition = action_str.split("] ")[1]
        from_ver, to_ver = transition.split(" --> ")
        app_updates_today.append((from_ver.strip(), to_ver.strip()))

    to_state = "Docker" if not apache2_installed and not postgres_installed else "Debian"

    new_install = None
    deleted_install = None
    valid_updates = []

    for from_ver, to_ver in app_updates_today:
      if from_ver == "NEW":
        new_install = to_ver
      elif to_ver == "DELETED":
        deleted_install = from_ver
      else:
        valid_updates.append((from_ver, to_ver))

    if new_install and deleted_install:
      valid_updates.append((deleted_install, new_install))

    for from_ver, to_ver in valid_updates:
      updates.append({
        "node_id": node_id,
        "date": date.isoformat(),
        "from_state": from_state,
        "to_state": to_state,
        "from_version": from_ver,
        "to_version": to_ver
      })

  return updates


def create_updates_df(downloads_dir: Path) -> pd.DataFrame:
  all_updates = []
  for file_path in find_all_version_logs(downloads_dir):
    all_updates.extend(parse_node_updates(file_path))
  return pd.DataFrame(all_updates)


def pivot_updates(df: pd.DataFrame) -> pd.DataFrame:
  # Zielversionen definieren
  versions = ["1.5.1", "1.6", "1.7"]

  # Eindeutige Node IDs als Basis
  node_ids = df["node_id"].unique()
  result = pd.DataFrame({"node_id": node_ids})

  for v in versions:
    # Zeilen finden, die diese Version als Ziel haben
    mask = df["to_version"].str.contains(v, na=False)
    subset = df[mask][["node_id", "date", "to_state"]]

    # Bei Mehrfachtreffern pro Node den letzten behalten
    subset = subset.drop_duplicates(subset=["node_id"], keep="last")

    # Spalten für dieses Pivot-Segment benennen
    subset = subset.rename(columns={
      "date": f"to{v}",
      "to_state": f"state{v}"
    })

    # An das Ergebnis-Dataframe mergen
    result = result.merge(subset, on="node_id", how="left")

  # Datumsspalten in datetime umwandeln für NaT Darstellung
  for v in versions:
    result[f"to{v}"] = pd.to_datetime(result[f"to{v}"], errors="coerce")

  return result.sort_values("node_id").reset_index(drop=True)


def parse_current_state_and_version(file_path: Path) -> dict:
  node_id = extract_node_id_from_path(file_path)

  try:
    with file_path.open(encoding="utf-8") as vf:
      data = json.load(vf)

      # Version auslesen (Priorität dwh-j2ee, Fallback ear)
      current_version = data.get("dwh-j2ee") or data.get("ear")

      # State auswerten
      apache_installed = data.get("apache2") != "[not installed]"
      postgres_installed = data.get("postgres") != "[not installed]"

      current_state = "Docker" if not apache_installed and not postgres_installed else "Debian"

      return {
        "node_id": node_id,
        "current_version": current_version,
        "current_state": current_state
      }
  except Exception:
    return {
      "node_id": node_id,
      "current_version": None,
      "current_state": None
    }


def create_current_versions_df(downloads_dir: Path) -> pd.DataFrame:
  rows = []
  for file_path in find_all_version_files(downloads_dir):
    rows.append(parse_current_state_and_version(file_path))
  return pd.DataFrame(rows)


def create_monitoring_start_df(base_csv: Path) -> pd.DataFrame:
  """
  Loads the base CSV and returns a DataFrame with node ID as string and the monitored_since date.
  """
  df = pd.read_csv(base_csv)
  df["node_id"] = df["node"].astype(str)
  return df[["node_id", "monitored_since"]].copy()


def merge_node_data(current_versions_df: pd.DataFrame,
    pivoted_updates_df: pd.DataFrame,
    monitoring_df: pd.DataFrame) -> pd.DataFrame:
  # Erster Merge: Versionen und Updates
  df = pd.merge(current_versions_df, pivoted_updates_df, on="node_id", how="outer")

  # Zweiter Merge: Monitoring Start Datum hinzufügen
  df = pd.merge(df, monitoring_df, on="node_id", how="outer")

  # Node ID für Sortierung numerisch machen
  df["node_id"] = pd.to_numeric(df["node_id"], errors="coerce")
  return df.sort_values("node_id").reset_index(drop=True)


def apply_cutoff_filter(merged_df: pd.DataFrame) -> pd.DataFrame:
  df = merged_df.copy()

  if "monitored_since" in df.columns:
    df["monitored_since"] = pd.to_datetime(df["monitored_since"], dayfirst=True, errors="coerce")
    cutoff = pd.to_datetime(CUTOFF_DATE)

    # Behalte nur Zeilen, die kleiner/gleich dem Cutoff sind oder NaT (fehlend)
    df = df[~(df["monitored_since"] > cutoff)]

  return df.reset_index(drop=True)


def get_major_releases_map(csv_path: Path) -> dict:
  df = pd.read_csv(csv_path, names=["version", "date", "type", "state"])

  # Explizit nur nach deb und docker filtern
  df = df[df["state"].isin(["deb", "docker"])]

  # Nur die MAJOR Releases behalten
  major_df = df[df["type"] == "MAJOR"]

  releases_map = {}

  for _, row in major_df.iterrows():
    raw_version = str(row["version"])

    base_version = None
    if "1.5" in raw_version:
      base_version = "1.5.1"
    elif "1.6" in raw_version:
      base_version = "1.6"
    elif "1.7" in raw_version:
      base_version = "1.7"

    if not base_version:
      continue

    state = "Docker" if row["state"] == "docker" else "Debian"

    if base_version not in releases_map:
      releases_map[base_version] = {}

    releases_map[base_version][state] = row["date"]

  return releases_map


def calculate_update_delay(df: pd.DataFrame, releases_map: dict) -> pd.DataFrame:
  df = df.copy()
  versions = ["1.5.1", "1.6", "1.7"]

  for v in versions:
    date_col = f"to{v}"
    state_col = f"state{v}"
    days_col = f"daysTo{v}"

    if date_col in df.columns and state_col in df.columns and v in releases_map:
      # Update-Datum umwandeln
      df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

      # Release-Datum basierend auf dem State der Zeile mappen
      release_dates = df[state_col].map(releases_map[v])
      release_dates = pd.to_datetime(release_dates, errors="coerce")

      # Differenz in Tagen berechnen
      df[days_col] = (df[date_col] - release_dates).dt.days

  return df


def generate_statistics_summary(df: pd.DataFrame, output_path: Path = None) -> str:
  lines = []
  df_clean = df.copy()

  def normalize_version(val):
    if pd.isna(val) or val == "":
      return "unknown"
    val_str = str(val)
    if "1.5" in val_str:
      return "1.5.1"
    if "1.6" in val_str:
      return "1.6"
    if "1.7" in val_str:
      return "1.7"
    return "unknown"

  df_clean["version_group"] = df_clean["current_version"].apply(normalize_version)
  total_nodes = len(df_clean)
  version_order = ["1.5.1", "1.6", "1.7", "unknown"]

  lines.append("Nodes per version:")
  for version in version_order:
    v_df = df_clean[df_clean["version_group"] == version]
    count = len(v_df)
    if count == 0:
      continue

    pct = (count / total_nodes) * 100

    state_str = ""
    # Aufschlüsselung nach State nur, wenn die Version bekannt ist
    if version != "unknown":
      state_counts = v_df["current_state"].value_counts().to_dict()
      state_details = []
      for state, s_count in state_counts.items():
        if pd.notna(state):
          state_details.append(f"{s_count} {state}")

      state_str = f" - {', '.join(state_details)}" if state_details else ""

    lines.append(f"{version}: {count} nodes ({pct:.2f}%){state_str}")

  def append_timing(series: pd.Series, label: str):
    values = series.dropna()
    count = len(values)
    if count == 0:
      return
    lines.append("")
    lines.append(f"Update timing for {label} of {count} nodes:")
    lines.append(f"Mean days: {values.mean():.2f}")
    lines.append(f"Median days: {values.median():.2f}")
    lines.append(f"Q1: {values.quantile(0.25):.2f}")
    lines.append(f"Q3: {values.quantile(0.75):.2f}")

  if "daysTo1.6" in df_clean.columns:
    append_timing(df_clean["daysTo1.6"], "1.6")
  if "daysTo1.7" in df_clean.columns:
    append_timing(df_clean["daysTo1.7"], "1.7")

  summary_name = Path(__file__).stem + ".txt"
  output_file = get_output_dir() / summary_name
  output_file.write_text("\n".join(lines), encoding="utf-8")
  derived_file = get_derived_dir() / summary_name
  derived_file.write_text("\n".join(lines), encoding="utf-8")


def main():
  base_csv = get_base_csv_file()
  releases_csv = get_releases_csv_file()
  downloads_dir = get_downloads_dir()
  output_dir = get_output_dir()

  df = create_updates_df(downloads_dir)

  pivoted_df = pivot_updates(df)

  current_versions_df = create_current_versions_df(downloads_dir)

  monitoring_df = create_monitoring_start_df(base_csv)

  merged_df = merge_node_data(current_versions_df, pivoted_df, monitoring_df)
  merged_df = apply_cutoff_filter(merged_df)

  releases = get_major_releases_map(releases_csv)
  merged_df = calculate_update_delay(merged_df, releases)

  generate_statistics_summary(merged_df)


if __name__ == "__main__":
  main()
