# aktin-dwh-operational-analytics

This repository contains analysis scripts used to process operational AKTIN infrastructure data and to generate selected manuscript tables and figures. The scripts cover descriptive analyses of
network growth, regional site distribution, EDIS vendor distribution, monitoring responsiveness, encounter summaries, software release timelines, and update uptake across participating AKTIN Data
Warehouse nodes.

## Repository structure

| Path          | Description                                                                                                                                                                              |
|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `src/`        | Contains the executable analysis scripts used to retrieve, process, and analyze operational data and to generate the manuscript figures and tables.                                      |
| `src/helper/` | Contains shared utility functions for common tasks such as path handling, data loading, output management, and access to external resources.                                             |
| `resources/`  | Static input resources required by the scripts. The `base.csv` file is not distributed with this repository.                                                                             |
| `downloads/`  | Auto-generated directory for files downloaded from external or internal sources during execution.                                                                                        |
| `derived/`    | Publicly distributed derived reproducibility data used as non-sensitive intermediate inputs for selected analyses. These files are aggregated outputs and not raw operational site data. |
| `out/`        | Auto-generated directory containing the final generated figures and tables.                                                                                                              |

## Requirements

- Python 3.12 or newer
- dependencies as defined in `pyproject.toml`

Install dependencies in your preferred environment (repository default `uv`) before running the scripts.

## Required environment variables

Several scripts require access to internal AKTIN services. These credentials are not part of the repository and must be provided through environment variables in the local runtime environment. These
secrets are confidential and are therefore not distributed with this repository.

| Variable           | Description                                                                                  |
|--------------------|----------------------------------------------------------------------------------------------|
| `CONFLUENCE_URL`   | Base URL of the Confluence instance used to download node-specific attachments.              |
| `CONFLUENCE_TOKEN` | Access token for the Confluence instance.                                                    |
| `BROKER_URL`       | Base URL of the AKTIN broker API, required by scripts that access internal broker resources. |
| `ADMIN_API_KEY`    | Administrative API key for authenticated broker access.                                      |

## Required local input file: `resources/base.csv`

Execution also requires a local `resources/base.csv` file. This file is not included in the repository because it contains sensitive operational metadata about participating clinical sites.

| Column            | Description                                                                                                                                                |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `node`            | Internal node identifier of the AKTIN site. This identifier is used to name per-node download folders and to link downloaded resources to a specific site. |
| `zipcode`         | Postal code of the site. This is used to map sites to geographic coordinates for the regional distribution figure.                                         |
| `state`           | German federal state of the site. This is used for aggregation and labeling in geographic summaries.                                                       |
| `edis`            | Reported Emergency Department Information System vendor of the site. This is used for the EDIS distribution analysis shown in the manuscript.              |
| `data_since`      | Start date of data contribution or operational inclusion for the site.                                                                                     |
| `monitored_since` | Start date of broker-based monitoring availability for the site.                                                                                           |
| `page_id`         | Confluence page identifier used to download attachments for a given site.                                                                                  |

## Workflow

The scripts are intended to be run individually, depending on the analysis to be reproduced. However, the repository has an implicit execution order:

1. Create or provide `resources/base.csv`.
2. Set the required environment variables.
3. Run `download_confluence_resources` first to populate the local baseline in `downloads/`.
4. Run the remaining analysis scripts as needed.
5. Collect generated figures and tables from `out/`.

Some scripts may also download additional helper resources on demand and store them in `downloads/`.

## Data access and reproducibility

This repository does not include the full underlying raw operational data, secret credentials, or the site-level `base.csv` file. Site-level operational data are not publicly distributed for data
protection and governance reasons. This includes files that contain or can reveal site-specific metadata, node-level monitoring records, internal identifiers, exact operational timestamps, or other
information that could enable re-identification of participating clinical sites.

To support transparent reuse without exposing sensitive site-level data, the repository provides a `derived/` directory containing aggregated and intermediate reproducibility files generated by the
analysis scripts. These files correspond to selected manuscript analyses and are named in alignment with the respective scripts that produced or consumed them. They are intended to allow reproduction
of the published tables and figures without redistributing confidential raw input data.

The `derived/` files reflect the analytical data state as of **2026-04-01**.

Additional data may be made available from the corresponding author (_akombeiz@ukaachen.de_) upon reasonable request and is subject to institutional approval. Access cannot be guaranteed and
will depend on the sensitivity of the requested data, the intended use, and the applicable regulatory and organizational constraints.

| File                          | Description                                                                                                                                                                        |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `edis_counts.csv`             | Aggregated counts of connected AKTIN emergency departments by reported Emergency Department Information System (EDIS) vendor after name normalization and grouping of rare systems |
| `csv_yearly_encounter.csv`    | Year-level summary table of reporting nodes, observed days, total emergency department encounters, per-node encounter statistics, and linked inpatient outcome coverage            |
| `linechart_connected_eds.csv` | Time series of cumulative numbers of connected AKTIN Data Warehouse nodes across the observation period                                                                            |
| `txt_node_updates.txt`        | Text summary of deployed software versions across connected nodes and descriptive statistics for update timing to version 1.6                                                      |

## License

This project is licensed under the Apache License 2.0. See the `LICENSE` file for details.
