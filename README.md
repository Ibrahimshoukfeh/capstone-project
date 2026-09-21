# Databricks Capstone Project

A medallion-architecture (bronze/silver/gold) data pipeline built on Databricks, deployed
with Databricks Asset Bundles. It ingests a raw sales dataset, cleans and validates it,
and produces gold-layer business metrics.

## Architecture

```
Volumes (raw files) --> Bronze --> Silver --> Gold
                        (raw)    (cleaned,   (aggregated
                                  validated)  metrics)
```

- **Volume to Bronze** (`code/notebooks/volumetobronze.py`) — loads raw files from a
  Unity Catalog volume into bronze tables, driven by a metadata table.
- **Bronze to Silver** (`code/notebooks/bronzetosilver.py`, `files/transformations/transformations.py`) —
  cleans each source table: normalizes date formats across multiple input formats,
  validates primary/foreign key columns against regex patterns, and applies
  table-specific transformations (customers, products, sales, stores, calendar).
- **Silver to Gold** (`code/notebooks/silvertogold.py`, `files/aggregations/aggregations.py`) —
  computes business metrics: monthly revenue metrics, lapsed customers, store
  performance, and customer loyalty tiers.
- **Environment prep** (`code/environment/catalog_prep.py`, `code/environment/metadata_loader.py`) —
  creates the Unity Catalog schemas (bronze/silver/gold/log/quarantine/config) and
  loads pipeline metadata from `files/metadata/metadata.yml`.
- **Logging** (`src/dbx_capstone_framework/logger.py`) — a reusable `EtlLogger` that
  writes run and error logs to Delta tables for observability across pipeline runs.

Table definitions, expected key formats, and transformation entry points are all
config-driven via `files/metadata/metadata.yml`, rather than hardcoded per table.

## Project layout

```
code/
  environment/     Catalog/schema setup and metadata loading
  notebooks/       Orchestration notebooks (volume->bronze->silver->gold)
  pipeline/        Pipeline entry points
files/
  transformations/ Silver-layer cleaning and validation logic
  aggregations/    Gold-layer business metrics
  metadata/        Table/pipeline configuration (metadata.yml)
src/
  dbx_capstone_framework/  Shared framework code (ETL logger)
deploy/
  bundle/          Databricks Asset Bundle deployment config
  pipeline/        Per-job Databricks Asset Bundle job definitions + CI pipeline
databricks.yml     Databricks Asset Bundle definition (targets: dev, uat)
```

## Deployment

This project is packaged as a [Databricks Asset Bundle](https://docs.databricks.com/en/dev-tools/bundles/index.html).

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

`deploy/pipeline/deploy_pipeline.yml` runs the same validate/deploy steps in CI.

## Status

Work in progress — some notebooks still contain placeholder values that need to be
filled in with the correct workspace paths before running end to end.
