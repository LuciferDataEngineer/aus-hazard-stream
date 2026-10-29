"""Dagster assets, a 6-hour schedule, a manifest sensor, and a daily partition.

Materialising ``refreshed_dashboard`` replays the snapshot, builds the dbt
marts, and writes the static dashboard. The scheduled GitHub Action is the
path that actually polls the feeds; this schedule is the same contract for
a local Dagster daemon.
"""

import json

import duckdb
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    DailyPartitionsDefinition,
    Definitions,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    asset,
    asset_check,
    define_asset_job,
    sensor,
)

from aus_hazard.config import MANIFEST, SNAPSHOTS

hotspot_days = DailyPartitionsDefinition(start_date="2026-10-26", end_date="2026-10-30")


@asset(partitions_def=hotspot_days, description="How many DEA detections fell on this UTC date.")
def hotspot_detections_for_day(context: AssetExecutionContext) -> dict[str, object]:
    day = context.partition_key
    path = SNAPSHOTS / "dea_hotspots.parquet"
    con = duckdb.connect()
    count = con.execute(
        """
        select count(*)
        from read_parquet(?)
        where cast(observed_at as timestamp)::date = cast(? as date)
        """,
        [str(path), day],
    ).fetchone()
    con.close()
    detections = int(count[0]) if count else 0
    context.log.info("hotspots on %s: %s", day, detections)
    return {"day": day, "detections": detections}


@asset(description="Replay the snapshot through the contract, dedup, and Iceberg path.")
def silver_catalog() -> dict[str, object]:
    from aus_hazard.pipeline import replay

    stats = replay()
    return {
        "unified_quakes": stats["unified_quakes"],
        "hotspots_out": stats["hotspots_out"],
        "dlq": stats["dlq"],
    }


@asset(deps=[silver_catalog], description="dbt marts on top of the silver Parquet.")
def dbt_marts() -> str:
    from aus_hazard.publish import export_marts, run_dbt

    run_dbt()
    export_marts()
    return "built"


@asset(deps=[dbt_marts], description="Static dashboard in dashboard/dist.")
def refreshed_dashboard() -> str:
    from aus_hazard.dashboard import build

    path = build()
    return str(path)


@asset_check(asset=silver_catalog, description="Dead-letter topic should not swallow the catalogue.")
def dead_letter_is_small() -> AssetCheckResult:
    stats_path = SNAPSHOTS.parent / "silver" / "replay_stats.json"
    if not stats_path.exists():
        return AssetCheckResult(passed=True, severity=AssetCheckSeverity.WARN, description="no replay yet")
    stats = json.loads(stats_path.read_text())
    dlq = int(stats.get("dlq") or 0)
    total = int(stats.get("ga_in") or 0) + int(stats.get("usgs_in") or 0) + int(stats.get("hotspots_in") or 0)
    passed = total == 0 or dlq / total < 0.01
    return AssetCheckResult(
        passed=passed,
        metadata={"dlq": dlq, "input_rows": total},
        description="Less than 1% of rows failed the contract.",
    )


refresh_job = define_asset_job(
    name="refresh_hazard_marts", selection=["silver_catalog", "dbt_marts", "refreshed_dashboard"]
)


@sensor(job=refresh_job, minimum_interval_seconds=300)
def manifest_sensor(context):  # type: ignore[no-untyped-def]
    if not MANIFEST.exists():
        yield SkipReason("manifest not fetched yet")
        return
    token = str(MANIFEST.stat().st_mtime_ns)
    if context.cursor == token:
        yield SkipReason("snapshot manifest unchanged")
        return
    context.update_cursor(token)
    yield RunRequest(run_key=token)


refresh_schedule = ScheduleDefinition(job=refresh_job, cron_schedule="0 */6 * * *")

defs = Definitions(
    assets=[hotspot_detections_for_day, silver_catalog, dbt_marts, refreshed_dashboard],
    asset_checks=[dead_letter_is_small],
    jobs=[refresh_job],
    schedules=[refresh_schedule],
    sensors=[manifest_sensor],
)
