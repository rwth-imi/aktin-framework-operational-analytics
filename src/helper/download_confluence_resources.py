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
Created on 7/2/25
@AUTHOR: Alexander Kombeiz (akombeiz@ukaachen.de)
@VERSION=1.0
"""

import csv
import os
from pathlib import Path

from atlassian import Confluence
from dotenv import load_dotenv

from paths import get_base_csv_file, get_downloads_dir

# Load environment variables from .env
load_dotenv()


def create_confluence_client() -> Confluence:
  """
  Create a Confluence client using URL and token from environment variables.
  """
  url = os.environ.get("CONFLUENCE_URL")
  token = os.environ.get("CONFLUENCE_TOKEN")
  return Confluence(url=url, token=token)


def ensure_node_folder_is_ready(node_folder: Path) -> bool:
  """
  Checks if the given folder already exists and has files. If it exists with files, returns False (skip download).
  Otherwise, creates the folder and returns True.
  """
  if node_folder.exists() and any(node_folder.iterdir()):
    return False
  node_folder.mkdir(parents=True, exist_ok=True)
  return True


def download_attachments_for_all_nodes(csv_file: Path, downloads_dir: Path, confluence: Confluence):
  """
  Read the base CSV and download attachments for each node from Confluence. Skips nodes that are already downloaded.
  """
  with open(csv_file, newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
      node_id = row["node"]
      page_id = row["page_id"]
      node_folder = downloads_dir / node_id
      if not ensure_node_folder_is_ready(node_folder):
        print(f"[SKIP] Node {node_id} already downloaded.")
        continue
      print(f"[INFO] Downloading attachments for Node {node_id} into {node_folder}...")
      confluence.download_attachments_from_page(page_id, path=str(node_folder))


def main():
  csv_file = get_base_csv_file()
  downloads_dir = get_downloads_dir()
  confluence = create_confluence_client()
  download_attachments_for_all_nodes(csv_file, downloads_dir, confluence)


if __name__ == "__main__":
  main()
