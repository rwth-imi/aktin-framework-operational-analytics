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
@VERSION=1.0
"""

from pathlib import Path


def get_project_root() -> Path:
  return Path(__file__).resolve().parents[2]


def get_base_csv_file() -> Path:
  return get_project_root() / "resources" / "base.csv"


def get_releases_csv_file() -> Path:
  return get_project_root() / "resources" / "releases.csv"


def get_modified_releases_csv_file() -> Path:
  return get_project_root() / "resources" / "modified_releases.csv"


def get_downloads_dir() -> Path:
  downloads_dir = get_project_root() / "downloads"
  downloads_dir.mkdir(parents=True, exist_ok=True)
  return downloads_dir


def get_output_dir() -> Path:
  output_dir = get_project_root() / "out"
  output_dir.mkdir(parents=True, exist_ok=True)
  return output_dir
