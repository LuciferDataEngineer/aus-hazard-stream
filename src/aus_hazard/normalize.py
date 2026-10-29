"""Map source payloads onto the versioned event contracts."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from aus_hazard.timeutil import iso, parse_time, to_millis


def _num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:  # NaN
        return None
    return number


def _epoch_or_time(value: Any) -> datetime | None:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value) / 1000, tz=UTC)
    return parse_time(value)


def _int(value: Any) -> int | None:
    number = _num(value)
    if number is None:
        return None
    return int(number)


def quake_from_usgs(feature: dict[str, Any], ingested_at: datetime) -> dict[str, Any] | None:
    props = feature.get("properties") or {}
    geom = (feature.get("geometry") or {}).get("coordinates") or []
    if len(geom) < 2 or props.get("mag") is None:
        return None
    origin = _epoch_or_time(props.get("time"))
    updated = _epoch_or_time(props.get("updated"))
    if origin is None or updated is None:
        return None
    event_id = str(feature.get("id") or props.get("code") or "")
    if not event_id:
        return None
    depth = _num(geom[2]) if len(geom) > 2 else None
    return {
        "schema_version": 1,
        "event_id": f"usgs:{event_id}",
        "source": "usgs",
        "source_event_id": event_id,
        "origin_time": iso(origin),
        "updated_time": iso(updated),
        "created_time": None,
        "latitude": float(geom[1]),
        "longitude": float(geom[0]),
        "depth_km": depth,
        "magnitude": float(props["mag"]),
        "mag_type": str(props.get("magType") or "unknown"),
        "place": str(props.get("place") or props.get("title") or ""),
        "status": str(props.get("status") or "automatic"),
        "event_type": str(props.get("type") or "earthquake"),
        "version": to_millis(updated),
        "felt_reports": _int(props.get("felt")),
        "url": props.get("url"),
        "ingested_at": iso(ingested_at),
    }


def quake_from_ga(feature: dict[str, Any], ingested_at: datetime) -> dict[str, Any] | None:
    props = feature.get("properties") or {}
    geom = (feature.get("geometry") or {}).get("coordinates") or []
    lat = _num(props.get("latitude"))
    lon = _num(props.get("longitude"))
    if lat is None or lon is None:
        if len(geom) >= 2:
            lon, lat = float(geom[0]), float(geom[1])
        else:
            return None
    origin = parse_time(props.get("origin_time"))
    if origin is None:
        return None
    # Placeholder rows in the unfiltered catalogue use year 2100.
    if origin.year > 2026 or origin.year < 1800:
        return None
    magnitude = _num(props.get("preferred_magnitude"))
    if magnitude is None:
        return None
    updated = parse_time(props.get("event_modification_time")) or origin
    created = parse_time(props.get("event_creation_time"))
    eq_id = str(props.get("earthquake_id") or props.get("origin_id") or "")
    if not eq_id:
        return None
    version = _int(props.get("published_version"))
    if version is None:
        version = to_millis(updated)
    place = str(props.get("description") or "")
    return {
        "schema_version": 1,
        "event_id": f"ga:{eq_id}",
        "source": "ga",
        "source_event_id": eq_id,
        "origin_time": iso(origin),
        "updated_time": iso(updated),
        "created_time": iso(created),
        "latitude": float(lat),
        "longitude": float(lon),
        "depth_km": _num(props.get("depth")),
        "magnitude": float(magnitude),
        "mag_type": str(props.get("preferred_magnitude_type") or "unknown"),
        "place": place,
        "status": str(props.get("evaluation_status") or "unknown"),
        "event_type": "earthquake",
        "version": int(version),
        "felt_reports": _int(props.get("felt_reports_count")),
        "url": None,
        "ingested_at": iso(ingested_at),
    }


def hotspot_from_dea(feature: dict[str, Any], ingested_at: datetime) -> dict[str, Any] | None:
    props = feature.get("properties") or {}
    geom = (feature.get("geometry") or {}).get("coordinates") or []
    lat = _num(props.get("latitude"))
    lon = _num(props.get("longitude"))
    if (lat is None or lon is None) and len(geom) >= 2:
        lon, lat = float(geom[0]), float(geom[1])
    if lat is None or lon is None:
        return None
    observed = parse_time(props.get("datetime") or props.get("start_dt"))
    if observed is None:
        return None
    raw_id = props.get("id")
    if raw_id is None:
        return None
    confidence = _int(props.get("confidence"))
    if confidence is None:
        confidence = 0
    confidence = max(0, min(100, confidence))
    processed = parse_time(props.get("process_dt"))
    loaded = parse_time(props.get("load_dt"))
    version_source = loaded or processed or observed
    return {
        "schema_version": 1,
        "hotspot_id": f"dea:{raw_id}",
        "source": "dea",
        "source_event_id": str(raw_id),
        "observed_at": iso(observed),
        "latitude": float(lat),
        "longitude": float(lon),
        "temp_kelvin": _num(props.get("temp_kelvin")),
        "power": _num(props.get("power")),
        "confidence": confidence,
        "state": props.get("australian_state") or None,
        "satellite": props.get("satellite"),
        "sensor": props.get("sensor"),
        "algorithm": props.get("process_algorithm"),
        "processed_at": iso(processed),
        "loaded_at": iso(loaded),
        "version": to_millis(version_source),
        "ingested_at": iso(ingested_at),
    }
