"""Replay the committed snapshot through the producer and consumer.

The in-memory bus stands in for Redpanda. Messages are still validated
against the versioned JSON Schema, dead-lettered on contract failure, and
upserted by event id and version. ``make stream`` uses the same functions
against Kafka when Redpanda is up.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pyarrow.parquet as pq

from aus_hazard.bus import InMemoryBus, Message
from aus_hazard.config import (
    GIPPSLAND,
    LINEAGE_DIR,
    QUAKE_LATENESS_SECONDS,
    SILVER,
    SNAPSHOTS,
    STDBSCAN_EPS_HOURS,
    STDBSCAN_EPS_KM,
    STDBSCAN_MIN_SAMPLES,
    TOPICS,
)
from aus_hazard.contracts import validate_hotspot, validate_quake
from aus_hazard.dedup import unify_catalogs, upsert_by_version
from aus_hazard.enrich import attach_h3, spatial_enrich, write_silver
from aus_hazard.geo import in_box
from aus_hazard.lake import commit_silver
from aus_hazard.stats import st_dbscan
from aus_hazard.timeutil import parse_time, to_millis
from aus_hazard.windows import Watermark


def _read(path: Any) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return pq.read_table(path).to_pylist()


def produce(bus: InMemoryBus, rows: list[dict[str, Any]], topic: str, key_field: str, kind: str) -> int:
    """Validate and publish. Invalid rows go to the dead-letter topic."""
    validator = validate_quake if kind == "quake" else validate_hotspot
    published = 0
    for row in rows:
        errors = validator(row)
        key = str(row.get(key_field) or "unknown")
        if errors:
            bus.publish(
                TOPICS["dlq"],
                key,
                {
                    "topic": topic,
                    "key": key,
                    "errors": errors,
                    "payload": row,
                    "failed_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                },
            )
            continue
        bus.publish(topic, key, row)
        published += 1
    return published


def consume(bus: InMemoryBus) -> dict[str, Any]:
    ga: dict[str, dict[str, Any]] = {}
    usgs: dict[str, dict[str, Any]] = {}
    hotspots: dict[str, dict[str, Any]] = {}
    actions = {"insert": 0, "update": 0, "stale": 0}
    quake_wm = Watermark(lateness_ms=QUAKE_LATENESS_SECONDS * 1000)
    for message in bus.read(TOPICS["quakes"]):
        row = message.value
        origin = parse_time(row["origin_time"])
        if origin is not None:
            row["arrival_status"] = quake_wm.observe(to_millis(origin))
        table = ga if row["source"] == "ga" else usgs
        action = upsert_by_version(table, row, "event_id")
        actions[action] += 1
    for message in bus.read(TOPICS["hotspots"]):
        action = upsert_by_version(hotspots, message.value, "hotspot_id")
        actions[action] += 1
    return {
        "ga": list(ga.values()),
        "usgs": list(usgs.values()),
        "hotspots": list(hotspots.values()),
        "dlq": [message.value for message in bus.read(TOPICS["dlq"])],
        "actions": actions,
        "watermark": {
            "late_events": quake_wm.late_events,
            "on_time_events": quake_wm.on_time_events,
            "lateness_seconds": QUAKE_LATENESS_SECONDS,
        },
    }


def _lag_seconds(start: str | None, end: str | None) -> int | None:
    a, b = parse_time(start), parse_time(end)
    if a is None or b is None:
        return None
    return int((b - a).total_seconds())


def _annotate_quakes(rows: list[dict[str, Any]]) -> None:
    box = GIPPSLAND
    for row in rows:
        row["in_gippsland_box"] = in_box(float(row["latitude"]), float(row["longitude"]), box)
        created = row.get("created_time")
        updated = row.get("updated_time")
        origin = row.get("origin_time")
        if row.get("sources") == "usgs":
            row["publication_lag_seconds"] = _lag_seconds(origin, updated)
            row["lag_kind"] = "last_update"
        else:
            lag = _lag_seconds(origin, created) if created else _lag_seconds(origin, updated)
            row["publication_lag_seconds"] = lag
            row["lag_kind"] = "creation" if created else "last_update"
        # Negative lags are clock or migration artefacts, not negative latency.
        if row["publication_lag_seconds"] is not None and row["publication_lag_seconds"] < 0:
            row["publication_lag_seconds"] = None
            row["lag_kind"] = "unusable"


def _annotate_hotspots(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        lag = _lag_seconds(row.get("observed_at"), row.get("processed_at"))
        if lag is not None and lag < 0:
            lag = None
        row["processing_lag_seconds"] = lag


def _project_quake(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": row.get("catalog_id"),
        "ga_id": row.get("ga_id"),
        "usgs_id": row.get("usgs_id"),
        "sources": row.get("sources"),
        "origin_time": row.get("origin_time"),
        "updated_time": row.get("updated_time"),
        "created_time": row.get("created_time"),
        "version": row.get("version"),
        "latitude": row.get("latitude"),
        "longitude": row.get("longitude"),
        "depth_km": row.get("depth_km"),
        "magnitude": row.get("magnitude"),
        "mag_type": row.get("mag_type"),
        "magnitude_usgs": row.get("magnitude_usgs"),
        "place": row.get("place"),
        "status": row.get("status"),
        "event_type": row.get("event_type"),
        "felt_reports": row.get("felt_reports"),
        "state_name": row.get("state_name"),
        "h3_res4": row.get("h3_res4"),
        "h3_res5": row.get("h3_res5"),
        "h3_res6": row.get("h3_res6"),
        "in_gippsland_box": bool(row.get("in_gippsland_box")),
        "publication_lag_seconds": row.get("publication_lag_seconds"),
        "lag_kind": row.get("lag_kind"),
        "match_distance_km": row.get("match_distance_km"),
        "match_time_delta_s": row.get("match_time_delta_s"),
        "arrival_status": row.get("arrival_status"),
        "cluster_id": row.get("cluster_id"),
        "ingested_at": row.get("ingested_at"),
        "url": row.get("url"),
    }


def _project_hotspot(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "hotspot_id": row.get("hotspot_id"),
        "observed_at": row.get("observed_at"),
        "latitude": row.get("latitude"),
        "longitude": row.get("longitude"),
        "temp_kelvin": row.get("temp_kelvin"),
        "power": row.get("power"),
        "confidence": row.get("confidence"),
        "state_name": row.get("state_name"),
        "satellite": row.get("satellite"),
        "sensor": row.get("sensor"),
        "algorithm": row.get("algorithm"),
        "processed_at": row.get("processed_at"),
        "loaded_at": row.get("loaded_at"),
        "processing_lag_seconds": row.get("processing_lag_seconds"),
        "h3_res4": row.get("h3_res4"),
        "h3_res5": row.get("h3_res5"),
        "h3_res6": row.get("h3_res6"),
        "nearest_place": row.get("nearest_place"),
        "nearest_place_pop": row.get("nearest_place_pop"),
        "nearest_place_km": row.get("nearest_place_km"),
        "nearest_airport": row.get("nearest_airport"),
        "nearest_airport_ident": row.get("nearest_airport_ident"),
        "nearest_airport_km": row.get("nearest_airport_km"),
        "near_population": bool(row.get("near_population")),
        "near_airport": bool(row.get("near_airport")),
        "ingested_at": row.get("ingested_at"),
        "version": row.get("version"),
    }


def _assign_clusters(quakes: list[dict[str, Any]]) -> dict[str, int]:
    points = []
    for row in quakes:
        if not row.get("in_gippsland_box"):
            row["cluster_id"] = None
            continue
        origin = parse_time(row["origin_time"])
        if origin is None:
            row["cluster_id"] = None
            continue
        points.append(
            (
                row["catalog_id"],
                float(row["latitude"]),
                float(row["longitude"]),
                origin.timestamp() / 3600,
            )
        )
    result = st_dbscan(points, STDBSCAN_EPS_KM, STDBSCAN_EPS_HOURS, STDBSCAN_MIN_SAMPLES)
    for row in quakes:
        if row.get("in_gippsland_box"):
            row["cluster_id"] = result.labels.get(row["catalog_id"])
    return {"n_clusters": result.n_clusters, "n_noise": result.n_noise, "n_points": len(points)}


def _write_lineage(stats: dict[str, Any]) -> None:
    LINEAGE_DIR.mkdir(parents=True, exist_ok=True)
    event = {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "producer": "aus-hazard-stream",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
        "job": {"namespace": "aus-hazard-stream", "name": "replay"},
        "inputs": [
            {"namespace": "snapshot", "name": "data/snapshots/ga_earthquakes.parquet"},
            {"namespace": "snapshot", "name": "data/snapshots/usgs_earthquakes.parquet"},
            {"namespace": "snapshot", "name": "data/snapshots/dea_hotspots.parquet"},
        ],
        "outputs": [
            {"namespace": "iceberg", "name": "silver.quakes"},
            {"namespace": "iceberg", "name": "silver.hotspots"},
            {"namespace": "parquet", "name": "data/silver/quakes.parquet"},
            {"namespace": "parquet", "name": "data/silver/hotspots.parquet"},
        ],
        "run": {"facets": {"stats": stats}},
    }
    (LINEAGE_DIR / "replay_openlineage.json").write_text(json.dumps(event, indent=2, default=str))


def replay(write_iceberg: bool = True) -> dict[str, Any]:
    ga_rows = _read(SNAPSHOTS / "ga_earthquakes.parquet")
    usgs_rows = _read(SNAPSHOTS / "usgs_earthquakes.parquet")
    hot_rows = _read(SNAPSHOTS / "dea_hotspots.parquet")
    # Event-time order: a historical backfill is not an out-of-order stream.
    # Out-of-order behaviour is covered by the watermark unit test and by
    # `make stream`, which publishes in arrival order.
    ga_rows.sort(key=lambda row: row["origin_time"])
    usgs_rows.sort(key=lambda row: row["origin_time"])
    hot_rows.sort(key=lambda row: row["observed_at"])

    bus = InMemoryBus()
    produce(bus, ga_rows, TOPICS["quakes"], "event_id", "quake")
    produce(bus, usgs_rows, TOPICS["quakes"], "event_id", "quake")
    produce(bus, hot_rows, TOPICS["hotspots"], "hotspot_id", "hotspot")
    consumed = consume(bus)
    unified = unify_catalogs(consumed["ga"], consumed["usgs"])
    _annotate_quakes(unified)
    _annotate_hotspots(consumed["hotspots"])
    attach_h3(unified)
    attach_h3(consumed["hotspots"])
    print(
        f"spatial join: quakes={len(unified)} hotspots={len(consumed['hotspots'])}",
        flush=True,
    )
    quakes, hotspots = spatial_enrich(unified, consumed["hotspots"])
    clusters = _assign_clusters(quakes)
    quakes = [_project_quake(row) for row in quakes]
    hotspots = [_project_hotspot(row) for row in hotspots]
    write_silver(quakes, hotspots, SILVER)
    iceberg = commit_silver(quakes, hotspots) if write_iceberg else {"skipped": True}
    stats = {
        "ga_in": len(ga_rows),
        "usgs_in": len(usgs_rows),
        "hotspots_in": len(hot_rows),
        "unified_quakes": len(quakes),
        "hotspots_out": len(hotspots),
        "matched_both_sources": sum(1 for row in quakes if row.get("sources") == "ga+usgs"),
        "dlq": len(consumed["dlq"]),
        "upserts": consumed["actions"],
        "watermark": consumed["watermark"],
        "clusters": clusters,
        "iceberg": iceberg,
        "topics": {topic: len(bus.read(topic)) for topic in (TOPICS["quakes"], TOPICS["hotspots"], TOPICS["dlq"])},
    }
    _write_lineage(stats)
    (SILVER / "replay_stats.json").write_text(json.dumps(stats, indent=2, default=str))
    print(
        json.dumps(
            {
                k: stats[k]
                for k in (
                    "ga_in",
                    "usgs_in",
                    "hotspots_in",
                    "unified_quakes",
                    "matched_both_sources",
                    "dlq",
                    "clusters",
                )
            },
            indent=2,
        )
    )
    return stats


def messages_from_rows(rows: list[dict[str, Any]], topic: str, key_field: str) -> list[Message]:
    return [Message(topic=topic, key=str(row[key_field]), value=row) for row in rows]


def main() -> None:
    replay()


if __name__ == "__main__":
    main()
