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
@VERSION=2.0.1
"""
import json
from pathlib import Path
from typing import List, Dict, Tuple

import pandas as pd

from helper.paths import get_base_csv_file, get_downloads_dir, get_output_dir, get_releases_csv_file, get_derived_dir

# log entries after the cutoff are ignored
CUTOFF_DATE = "2026-04-01"
TARGET_VERSIONS = ["1.5.1", "1.6", "1.7"]


def extract_node_id(file_path: Path) -> str:
  return file_path.parent.name


def find_files_by_suffix(directory: Path, keyword: str, suffix: str) -> List[Path]:
  return sorted(
      f for d in directory.iterdir() if d.is_dir()
      for f in d.iterdir() if f.is_file() and keyword in f.name and f.suffix == suffix
  )


def determine_state(apache_installed: bool, postgres_installed: bool) -> str:
  """Determines if the system is a Docker or Debian environment based on installed packages."""
  if not apache_installed and not postgres_installed:
    return "Docker"
  return "Debian"


def normalize_version(val: str) -> str:
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


def read_and_group_logs(file_path: Path) -> Dict[str, List[str]]:
  """Reads a log file line by line and groups the parsed actions chronologically by date."""
  parsed_lines = []
  with file_path.open(encoding="utf-8") as f:
    for line in f:
      if " : " not in line:
        continue
      time_str, action_str = line.strip().split(" : ", 1)
      date = pd.to_datetime(time_str, utc=True, errors="coerce").date()
      if pd.notna(date):
        parsed_lines.append((date, action_str))
  parsed_lines.sort(key=lambda x: x[0])
  actions_by_date = {}
  for date, action in parsed_lines:
    actions_by_date.setdefault(date, []).append(action)
  return actions_by_date


def process_daily_actions(actions: List[str], apache_inst: bool, postgres_inst: bool) -> Tuple[
  bool, bool, List[Tuple[str, str]]]:
  """Evaluates a single day's actions to track the system state and application updates."""
  app_updates = []
  for action in actions:
    if "[apache2]" in action:
      apache_inst = "[not installed]" not in action.split("-->")[1]
    if "[postgres]" in action:
      postgres_inst = "[not installed]" not in action.split("-->")[1]
    if "[dwh-j2ee]" in action or "[ear]" in action:
      transition = action.split("] ")[1]
      from_ver, to_ver = transition.split(" --> ")
      app_updates.append((from_ver.strip(), to_ver.strip()))
  return apache_inst, postgres_inst, app_updates


def resolve_install_transitions(app_updates: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
  """Cleans up a day's updates by merging or filtering new installations and deletions."""
  new_install = None
  deleted_install = None
  valid_updates = []
  for from_ver, to_ver in app_updates:
    if from_ver == "NEW":
      new_install = to_ver
    elif to_ver == "DELETED":
      deleted_install = from_ver
    else:
      valid_updates.append((from_ver, to_ver))
  if new_install and deleted_install:
    valid_updates.append((deleted_install, new_install))
  return valid_updates


def parse_node_updates(file_path: Path) -> List[dict]:
  """Analyzes the full history of a node and returns all cleaned updates with their respective states."""
  node_id = extract_node_id(file_path)
  actions_by_date = read_and_group_logs(file_path)
  apache_installed = True
  postgres_installed = True
  updates = []
  for date, actions in actions_by_date.items():
    from_state = determine_state(apache_installed, postgres_installed)
    apache_installed, postgres_installed, app_updates = (
      process_daily_actions(actions, apache_installed,
                            postgres_installed))
    to_state = determine_state(apache_installed, postgres_installed)
    valid_updates = resolve_install_transitions(app_updates)
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


def parse_current_state_and_version(file_path: Path) -> dict:
  """Reads the current version and system state of a node from its versions JSON file."""
  node_id = extract_node_id(file_path)
  try:
    with file_path.open(encoding="utf-8") as vf:
      data = json.load(vf)
      current_version = data.get("dwh-j2ee") or data.get("ear")
      apache_installed = data.get("apache2") != "[not installed]"
      postgres_installed = data.get("postgres") != "[not installed]"
      return {
        "node_id": node_id,
        "current_version": current_version,
        "current_state": determine_state(apache_installed, postgres_installed)
      }
  except Exception:
    return {
      "node_id": node_id,
      "current_version": None,
      "current_state": None
    }


def create_updates_df(downloads_dir: Path) -> pd.DataFrame:
  """Creates an aggregated dataframe containing all historical updates across all nodes."""
  all_updates = [
    update
    for file_path in find_files_by_suffix(downloads_dir, "log_versions", ".log")
    for update in parse_node_updates(file_path)
  ]
  return pd.DataFrame(all_updates)


def create_current_versions_df(downloads_dir: Path) -> pd.DataFrame:
  """Creates an aggregated dataframe with the currently installed versions for all nodes."""
  rows = [
    parse_current_state_and_version(file_path)
    for file_path in find_files_by_suffix(downloads_dir, "versions", ".txt")
  ]
  return pd.DataFrame(rows)


def create_monitoring_start_df(base_csv: Path) -> pd.DataFrame:
  """Loads the monitoring start date for each node from the base CSV file."""
  df = pd.read_csv(base_csv)
  df["node_id"] = df["node"].astype(str)
  return df[["node_id", "monitored_since"]].copy()


def pivot_updates(df: pd.DataFrame) -> pd.DataFrame:
  """Transposes the update history so each target version gets its own date and state column."""
  node_ids = df["node_id"].unique()
  result = pd.DataFrame({"node_id": node_ids})
  for v in TARGET_VERSIONS:
    mask = df["to_version"].str.contains(v, na=False)
    subset = df[mask][["node_id", "date", "to_state"]]
    subset = subset.drop_duplicates(subset=["node_id"], keep="last")
    subset = subset.rename(columns={"date": f"to{v}", "to_state": f"state{v}"})
    result = result.merge(subset, on="node_id", how="left")
    result[f"to{v}"] = pd.to_datetime(result[f"to{v}"], errors="coerce")
  return result.sort_values("node_id").reset_index(drop=True)


def merge_node_data(current_versions_df: pd.DataFrame, pivoted_updates_df: pd.DataFrame,
    monitoring_df: pd.DataFrame) -> pd.DataFrame:
  """Merges the current versions, transposed updates, and monitoring data into a single dataframe."""
  df = pd.merge(current_versions_df, pivoted_updates_df, on="node_id", how="outer")
  df = pd.merge(df, monitoring_df, on="node_id", how="outer")
  df["node_id"] = pd.to_numeric(df["node_id"], errors="coerce")
  return df.sort_values("node_id").reset_index(drop=True)


def apply_cutoff_filter(df: pd.DataFrame) -> pd.DataFrame:
  """Removes all nodes from the dataset whose monitoring start date is after the defined cutoff date."""
  if "monitored_since" not in df.columns:
    return df
  df = df.copy()
  df["monitored_since"] = pd.to_datetime(df["monitored_since"], dayfirst=True, errors="coerce")
  cutoff = pd.to_datetime(CUTOFF_DATE)
  df = df[~(df["monitored_since"] > cutoff)]
  return df.reset_index(drop=True)


def get_major_releases_map(csv_path: Path) -> dict:
  """Creates a dictionary mapping official release dates for Debian and Docker per major version."""
  df = pd.read_csv(csv_path, names=["version", "date", "type", "state"])
  df = df[df["state"].isin(["deb", "docker"]) & (df["type"] == "MAJOR")]
  releases_map = {}
  for _, row in df.iterrows():
    base_version = normalize_version(str(row["version"]))
    if base_version == "unknown":
      continue
    state = "Docker" if row["state"] == "docker" else "Debian"
    releases_map.setdefault(base_version, {})[state] = row["date"]
  return releases_map


def calculate_update_delay(df: pd.DataFrame, releases_map: dict) -> pd.DataFrame:
  """Calculates the difference in days between a node's update and the official release date."""
  df = df.copy()
  for v in TARGET_VERSIONS:
    date_col = f"to{v}"
    state_col = f"state{v}"
    days_col = f"daysTo{v}"
    if date_col in df.columns and state_col in df.columns and v in releases_map:
      df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
      release_dates = pd.to_datetime(df[state_col].map(releases_map[v]), errors="coerce")
      df[days_col] = (df[date_col] - release_dates).dt.days
  return df


def generate_statistics_summary(df: pd.DataFrame) -> None:
  """Generates a statistical summary of the updates and saves it as a text file."""
  lines = []
  df_clean = df.copy()
  df_clean["version_group"] = df_clean["current_version"].apply(normalize_version)
  total_nodes = len(df_clean)
  lines.append("Nodes per version:")
  for version in TARGET_VERSIONS + ["unknown"]:
    v_df = df_clean[df_clean["version_group"] == version]
    count = len(v_df)
    if count == 0:
      continue
    pct = (count / total_nodes) * 100
    state_str = ""
    if version != "unknown":
      state_counts = v_df["current_state"].value_counts().to_dict()
      state_details = [f"{c} {s}" for s, c in state_counts.items() if pd.notna(s)]
      state_str = f" - {', '.join(state_details)}" if state_details else ""
    lines.append(f"{version}: {count} nodes ({pct:.2f}%){state_str}")

  def append_timing(series: pd.Series, label: str):
    values = series.dropna()
    if len(values) == 0:
      return
    lines.extend([
      "",
      f"Update timing for {label} of {len(values)} nodes:",
      f"Mean days: {values.mean():.2f}",
      f"Median days: {values.median():.2f}",
      f"Q1: {values.quantile(0.25):.2f}",
      f"Q3: {values.quantile(0.75):.2f}"
    ])

  for v in ["1.6", "1.7"]:
    days_col = f"daysTo{v}"
    if days_col in df_clean.columns:
      append_timing(df_clean[days_col], v)
  summary_text = "\n".join(lines)
  summary_name = Path(__file__).stem + ".txt"
  (get_output_dir() / summary_name).write_text(summary_text, encoding="utf-8")
  (get_derived_dir() / summary_name).write_text(summary_text, encoding="utf-8")


def main():
  downloads_dir = get_downloads_dir()
  updates_df = create_updates_df(downloads_dir)
  pivoted_updates_df = pivot_updates(updates_df)
  current_versions_df = create_current_versions_df(downloads_dir)
  monitoring_df = create_monitoring_start_df(get_base_csv_file())
  merged_df = merge_node_data(current_versions_df, pivoted_updates_df, monitoring_df)
  merged_df = apply_cutoff_filter(merged_df)
  releases_map = get_major_releases_map(get_releases_csv_file())
  final_df = calculate_update_delay(merged_df, releases_map)
  generate_statistics_summary(final_df)


if __name__ == "__main__":
  main()
