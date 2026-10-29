"""Run dbt, then copy marts out of the DuckDB file as Parquet."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

from aus_hazard.config import ANALYTICS_DB, MARTS, ROOT

MART_TABLES = [
    "fct_earthquake",
    "fct_hotspot",
    "dim_region",
    "dim_h3_cell",
    "mart_swarm_activity",
    "mart_hazard_by_h3_week",
    "mart_feed_latency_freshness",
    "mart_magnitude_frequency",
]


def run_dbt(select: str | None = None) -> None:
    os.environ["AUS_HAZARD_ROOT"] = str(ROOT)
    from dbt.cli.main import dbtRunner

    args = ["build", "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(ROOT / "dbt")]
    if select:
        args.extend(["--select", select])
    result = dbtRunner().invoke(args)
    if not result.success:
        raise SystemExit("dbt build failed")


def export_marts(database: Path = ANALYTICS_DB, destination: Path = MARTS) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(database), read_only=True)
    for name in MART_TABLES:
        target = destination / f"{name}.parquet"
        con.execute(f"copy (select * from {name}) to '{target}' (format parquet, compression zstd)")
    con.close()


def main() -> None:
    run_dbt()
    export_marts()


if __name__ == "__main__":
    main()
