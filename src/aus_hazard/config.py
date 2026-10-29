"""Shared paths, bounding boxes, and feed definitions."""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(os.environ.get("AUS_HAZARD_ROOT", Path(__file__).resolve().parents[2]))
DATA = ROOT / "data"
SNAPSHOTS = DATA / "snapshots"
REFERENCE = DATA / "reference"
SAMPLES = DATA / "samples"
SILVER = DATA / "silver"
MARTS = DATA / "marts"
ICEBERG_WAREHOUSE = DATA / "iceberg"
CATALOG_DB = DATA / "catalog.db"
ANALYTICS_DB = DATA / "analytics.duckdb"
MANIFEST = DATA / "manifest.json"
LINEAGE_DIR = DATA / "lineage"
CONTRACTS = ROOT / "contracts"
DASHBOARD_DIST = ROOT / "dashboard" / "dist"

# Australian analysis window used for USGS (lon/lat). Includes the continent,
# the southern margin, and near-offshore events. New Zealand's main seismic
# belts sit east of 160E and are intentionally outside this box.
BBOX = {
    "min_latitude": -45.0,
    "max_latitude": -9.0,
    "min_longitude": 110.0,
    "max_longitude": 160.0,
}

# South Gippsland box around the 2026 Leongatha / Arawata sequence.
# Chosen to cover the tight cluster near Leongatha (~145.9E, 38.4S) with
# margin, not the whole Gippsland basin.
GIPPSLAND = {
    "name": "South Gippsland",
    "min_lat": -38.70,
    "max_lat": -38.15,
    "min_lon": 145.55,
    "max_lon": 146.30,
    "swarm_start": "2026-09-26T00:00:00Z",
}

HISTORICAL_START = "2006-01-01T00:00:00Z"
CATALOG_END = "2026-10-30T00:00:00Z"

TOPICS = {
    "quakes": "quakes.raw",
    "hotspots": "hotspots.raw",
    "dlq": "hazards.dlq",
}

# Event-time lateness allowance. Revisions that arrive after this are tagged
# late but still upserted — earthquake solutions are revised on purpose.
QUAKE_LATENESS_SECONDS = 6 * 3600
HOTSPOT_LATENESS_SECONDS = 2 * 3600

# Cross-catalog match tolerances. GA and USGS locations for the same
# Australian event often differ by tens of kilometres, and magnitude types
# (MLa vs mb) are not identical.
MATCH_MAX_SECONDS = 120
MATCH_MAX_KM = 100.0
MATCH_MAX_DMAG = 1.0

# ST-DBSCAN parameters for the Gippsland box. See stats.py for why these
# values are descriptive, not a physical model of rupture.
STDBSCAN_EPS_KM = 25.0
STDBSCAN_EPS_HOURS = 24.0 * 7
STDBSCAN_MIN_SAMPLES = 4

NEAR_PLACE_KM = 25.0
NEAR_AIRPORT_KM = 20.0
PLACE_POP_MIN = 10_000

# Freshness / publication SLAs shown on the dashboard.
SLA = {
    "ga_publication_minutes": {"green": 15, "amber": 60},
    "usgs_update_minutes": {"green": 30, "amber": 180},
    "hotspot_processing_minutes": {"green": 45, "amber": 120},
    "hotspot_freshness_hours": {"green": 3, "amber": 12},
    "quake_ingest_hours": {"green": 12, "amber": 48},
}

USER_AGENT = "aus-hazard-stream/0.1 (github.com/LuciferDataEngineer; portfolio; no-key public data)"
