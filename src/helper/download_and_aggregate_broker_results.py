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
Created on 7/18/25
@AUTHOR: Alexander Kombeiz (akombeiz@ukaachen.de)
@VERSION=1.0
"""

import os
import shutil
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path

import requests
from dotenv import load_dotenv
from requests.structures import CaseInsensitiveDict

# Load environment variables from .env
load_dotenv()


def append_to_broker_url(*items: str) -> str:
  """
  Builds the full API URL by appending path parts to the base BROKER_URL.
  """
  broker_url = os.environ.get("BROKER_URL")
  return "/".join([broker_url] + list(items))


def create_basic_header(mediatype: str = None, accept: str = None) -> CaseInsensitiveDict[str]:
  """
  Creates the HTTP header for API calls to the AKTIN Broker.
  Adds Authorization and optional Content-Type and Accept.
  """
  admin_api_key = os.environ.get("ADMIN_API_KEY")
  headers = requests.utils.default_headers()
  if mediatype:
    headers["Content-Type"] = mediatype
  if accept:
    headers["Accept"] = accept
  headers["Authorization"] = f"Bearer {admin_api_key}"
  return headers


def export_request_result(request_id: str) -> str:
  """
  Sends a request to the broker to prepare an export for a given request_id.
  Returns a UUID for downloading.
  """
  url = append_to_broker_url("broker", "export", "request-bundle", request_id)
  response = requests.post(url, headers=create_basic_header(accept="text/plain"), timeout=5)
  response.raise_for_status()
  return response.text


def download_exported_result(uuid: str) -> BytesIO:
  """
  Downloads the exported ZIP bundle using the UUID.
  """
  url = append_to_broker_url("broker", "download", uuid)
  response = requests.get(url, headers=create_basic_header(), timeout=5)
  response.raise_for_status()
  return BytesIO(response.content)


def extract_export_zip(zip_bytes: BytesIO, target_dir: Path, overwrite: bool = False):
  """
  Extracts all *_result.zip files from a ZIP archive (in-memory) into target_dir.
  If overwrite=True, existing files in target_dir will be replaced.
  """
  with zipfile.ZipFile(zip_bytes) as zf:
    for name in zf.namelist():
      if name.endswith("_result.zip"):
        out_path = target_dir / name
        if overwrite or not out_path.exists():
          with zf.open(name) as src, open(out_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
          print(f"{'Patched' if overwrite else 'Added'} {name}")


def pack_dir_to_zip(source_dir: Path, zip_path: Path):
  """
  Packs all files from source_dir into a new ZIP archive at zip_path.
  """
  with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
    for file in source_dir.iterdir():
      zf.write(file, arcname=file.name)


def get_or_export_request_result(downloads_dir: Path, request_ids: list[str]) -> Path:
  """
  Checks if a merged export ZIP (based on the first request ID) already exists. If not, downloads all request results.
  Uses the first ID as the base and overwrites specific node files with patches from the following IDs.
  Returns path to the final merged ZIP.
  """
  main_id = request_ids[0]
  cache_file = downloads_dir / f"export_{main_id}.zip"
  if cache_file.exists():
    print(f"Loading cached export from {cache_file}")
    return cache_file

  print("Cache not found, downloading main and patch exports...")
  with tempfile.TemporaryDirectory() as tmp:
    tmp_path = Path(tmp)
    merged_dir = tmp_path / "merged"
    merged_dir.mkdir()

    # Download and extract all exports
    for i, request_id in enumerate(request_ids):
      uuid = export_request_result(request_id)
      zip_bytes = download_exported_result(uuid)
      # Overwrite main on patches
      extract_export_zip(zip_bytes, merged_dir, overwrite=(i > 0))
    # Repack merged folder into final ZIP
    pack_dir_to_zip(merged_dir, cache_file)
  print(f"Saved result export to {cache_file}")
  return cache_file
