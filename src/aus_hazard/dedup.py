"""Idempotent version upserts and GA/USGS cross-catalog matching."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

from aus_hazard.config import MATCH_MAX_DMAG, MATCH_MAX_KM, MATCH_MAX_SECONDS
from aus_hazard.geo import haversine_km
from aus_hazard.timeutil import parse_time


def upsert_by_version(table: dict[str, dict[str, Any]], row: dict[str, Any], id_field: str) -> str:
    """Keep the highest version for an event id. Equal versions replace, so a
    replay is idempotent and a revised solution wins over an earlier one."""
    key = row[id_field]
    previous = table.get(key)
    if previous is None:
        table[key] = row
        return "insert"
    if int(row["version"]) >= int(previous["version"]):
        table[key] = row
        return "update"
    return "stale"


def _bucket(moment: datetime) -> int:
    return int(moment.timestamp()) // 300


def unify_catalogs(
    ga_rows: list[dict[str, Any]],
    usgs_rows: list[dict[str, Any]],
    max_seconds: int = MATCH_MAX_SECONDS,
    max_km: float = MATCH_MAX_KM,
    max_dmag: float = MATCH_MAX_DMAG,
) -> list[dict[str, Any]]:
    """Match USGS to GA. GA wins location and magnitude when both saw the event.

    Magnitude types differ (local ML versus teleseismic mb), so the tolerance
    is wide on purpose. Unmatched rows are kept with their own source id.
    """
    buckets: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in ga_rows:
        origin = parse_time(row["origin_time"])
        if origin is None:
            continue
        buckets[_bucket(origin)].append(row)

    used_ga: set[str] = set()
    unified: list[dict[str, Any]] = []

    for usgs in usgs_rows:
        origin = parse_time(usgs["origin_time"])
        if origin is None:
            continue
        bucket = _bucket(origin)
        best: tuple[float, float, dict[str, Any]] | None = None
        for offset in (-1, 0, 1):
            for ga in buckets.get(bucket + offset, []):
                if ga["event_id"] in used_ga:
                    continue
                ga_origin = parse_time(ga["origin_time"])
                if ga_origin is None:
                    continue
                delta_s = abs((ga_origin - origin).total_seconds())
                if delta_s > max_seconds:
                    continue
                distance = haversine_km(ga["latitude"], ga["longitude"], usgs["latitude"], usgs["longitude"])
                if distance > max_km:
                    continue
                if abs(float(ga["magnitude"]) - float(usgs["magnitude"])) > max_dmag:
                    continue
                if best is None or (distance, delta_s) < (best[0], best[1]):
                    best = (distance, delta_s, ga)
        if best is None:
            unified.append(_from_single(usgs, "usgs"))
            continue
        distance, delta_s, ga = best
        used_ga.add(ga["event_id"])
        unified.append(_merge(ga, usgs, distance, delta_s))

    for ga in ga_rows:
        if ga["event_id"] not in used_ga:
            unified.append(_from_single(ga, "ga"))
    return unified


def _from_single(row: dict[str, Any], source: str) -> dict[str, Any]:
    out = dict(row)
    out["catalog_id"] = row["event_id"]
    out["ga_id"] = row["source_event_id"] if source == "ga" else None
    out["usgs_id"] = row["source_event_id"] if source == "usgs" else None
    out["sources"] = source
    out["magnitude_usgs"] = float(row["magnitude"]) if source == "usgs" else None
    out["match_distance_km"] = None
    out["match_time_delta_s"] = None
    return out


def _merge(ga: dict[str, Any], usgs: dict[str, Any], distance_km: float, delta_s: float) -> dict[str, Any]:
    # Local-network solution is the canonical row. USGS is lineage.
    out = dict(ga)
    out["catalog_id"] = ga["event_id"]
    out["ga_id"] = ga["source_event_id"]
    out["usgs_id"] = usgs["source_event_id"]
    out["sources"] = "ga+usgs"
    out["magnitude_usgs"] = float(usgs["magnitude"])
    out["match_distance_km"] = round(distance_km, 3)
    out["match_time_delta_s"] = round(delta_s, 3)
    if not out.get("url"):
        out["url"] = usgs.get("url")
    felt = usgs.get("felt_reports")
    if felt and not out.get("felt_reports"):
        out["felt_reports"] = felt
    # Version for the unified row stays the GA published version. The USGS
    # updated-time is preserved on the bronze USGS record.
    return out
