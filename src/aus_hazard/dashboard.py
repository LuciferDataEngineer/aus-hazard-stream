"""Build a self-contained static dashboard from the dbt marts."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from aus_hazard.config import ANALYTICS_DB, DASHBOARD_DIST, SLA
from aus_hazard.geo import cell_polygon

TEMPLATE = (Path(__file__).resolve().parents[2] / "dashboard" / "template.html").read_text()


def _tone_status(status: str | None) -> str:
    return {"unusual": "bad", "elevated": "warn", "typical": "ok"}.get(status or "", "warn")


def _tone_minutes(value: float | None, green: float, amber: float) -> str:
    if value is None:
        return "warn"
    if value <= green:
        return "ok"
    if value <= amber:
        return "warn"
    return "bad"


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    table = con.execute(sql).to_arrow_table()
    assert table is not None
    rows = table.to_pylist()
    for row in rows:
        for key, value in list(row.items()):
            if hasattr(value, "isoformat"):
                row[key] = value.isoformat()
    return rows


def _hexes(con: duckdb.DuckDBPyConnection, sql: str, value_key: str) -> dict:
    features = []
    for row in _rows(con, sql):
        cell = row["h3_res4"]
        if not cell:
            continue
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "h3": cell,
                    "value": row[value_key],
                    "label": row.get("label") or "",
                },
                "geometry": {"type": "Polygon", "coordinates": [cell_polygon(cell)]},
            }
        )
    return {"type": "FeatureCollection", "features": features}


def build_payload(database: Path = ANALYTICS_DB) -> dict:
    con = duckdb.connect(str(database), read_only=True)
    swarm = _rows(con, "select * from mart_swarm_activity")
    by_scope = {row["scope"]: row for row in swarm}
    gipp = by_scope.get("south_gippsland_swarm") or {}
    g7 = by_scope.get("south_gippsland_7d") or {}
    g30 = by_scope.get("south_gippsland_30d") or {}
    latency = _rows(con, "select * from mart_feed_latency_freshness")
    by_feed = {row["feed"]: row for row in latency}
    mag = _rows(
        con,
        """
        select scope, mag_bin, n, log10_n
        from mart_magnitude_frequency
        order by scope, mag_bin
        """,
    )
    largest = _rows(
        con,
        """
        select place, magnitude, mag_type, origin_time, state_name, depth_km, sources
        from fct_earthquake
        where origin_time >= (select max(origin_time) from fct_earthquake) - interval 30 day
          and latitude <= -11
          and longitude between 112 and 155
        order by magnitude desc, state_name is not null desc, origin_time desc
        limit 1
        """,
    )
    weekly = _rows(
        con,
        """
        with latest as (select max(origin_time) as t from fct_earthquake)
        select date_trunc('week', origin_time) as week, count(*) as n
        from fct_earthquake, latest
        where magnitude >= 2
          and origin_time >= latest.t - interval 3 year
        group by 1
        order by 1
        """,
    )
    baseline_week = con.execute(
        """
        with weeks as (
            select week_start
            from generate_series(timestamp '2006-01-02', timestamp '2026-09-19', interval 7 day) t(week_start)
        ),
        counts as (
            select w.week_start, count(q.event_id) as n
            from weeks w
            left join fct_earthquake q
              on q.magnitude >= 2
             and q.origin_time >= w.week_start
             and q.origin_time < w.week_start + interval 7 day
            group by 1
        )
        select median(n) from counts
        """
    ).fetchone()
    states = _rows(
        con,
        """
        select coalesce(state_name, 'Unassigned') as state,
               count(*) as detections,
               count(*) filter (where near_population) as near_population,
               count(*) filter (where near_airport) as near_airport
        from fct_hotspot
        group by 1
        order by detections desc
        """,
    )
    trend = _rows(
        con,
        """
        select time_bucket(interval '6 hours', observed_at) as bucket,
               coalesce(state_name, 'Unassigned') as state,
               count(*) as n
        from fct_hotspot
        group by 1, 2
        order by 1
        """,
    )
    swarm_events = _rows(
        con,
        """
        select origin_time, magnitude, depth_km, place, mag_type, cluster_id
        from fct_earthquake
        where in_gippsland_box
          and origin_time >= timestamp '2026-09-01'
        order by origin_time
        """,
    )
    hotspot_summary = _rows(
        con,
        """
        with latest as (select max(observed_at) as t, max(ingested_at) as ingested from fct_hotspot)
        select
            (select count(*) from fct_hotspot) as detections_72h,
            (
                select count(*) from fct_hotspot, latest
                where observed_at >= latest.t - interval 6 hour
            ) as detections_6h,
            (
                select count(distinct h3_res6) from fct_hotspot, latest
                where observed_at >= latest.t - interval 6 hour
            ) as cells_6h,
            (select count(*) from fct_hotspot where near_population) as near_population,
            (select count(*) from fct_hotspot where near_airport) as near_airport,
            (select count(*) from fct_hotspot where nearest_airport_ident = 'YPPH' and nearest_airport_km <= 25) as near_perth_airport,
            (select min(nearest_airport_km) from fct_hotspot where nearest_airport_ident = 'YPPH') as closest_perth_airport_km,
            (select max(observed_at) from fct_hotspot) as last_hotspot,
            (select max(ingested_at) from fct_hotspot) as ingested
        """,
    )[0]
    quake_hex = _hexes(
        con,
        """
        with latest as (select max(origin_time) as t from fct_earthquake)
        select h3_res4, count(*) as value,
               count(*)::varchar || ' quakes, max M' || round(max(magnitude), 1)::varchar as label
        from fct_earthquake, latest
        where magnitude >= 2
          and origin_time >= latest.t - interval 365 day
        group by 1
        having count(*) > 0
        """,
        "value",
    )
    hot_hex = _hexes(
        con,
        """
        select h3_res4, count(*) as value,
               count(*)::varchar || ' detections' as label
        from fct_hotspot
        group by 1
        order by count(*) desc
        limit 400
        """,
        "value",
    )
    quake_points = _rows(
        con,
        """
        select longitude, latitude, magnitude, place, origin_time
        from fct_earthquake
        where in_gippsland_box and origin_time >= timestamp '2026-09-26'
        """,
    )
    catalog = _rows(
        con,
        """
        select
            (select count(*) from fct_earthquake) as quakes,
            (select count(*) from fct_earthquake where sources = 'ga+usgs') as matched,
            (select min(origin_time) from fct_earthquake) as quake_min,
            (select max(origin_time) from fct_earthquake) as quake_max,
            (select min(observed_at) from fct_hotspot) as hot_min,
            (select max(observed_at) from fct_hotspot) as hot_max
        """,
    )[0]
    con.close()

    ga = by_feed.get("geoscience_australia") or {}
    dea = by_feed.get("dea_hotspots") or {}
    usgs = by_feed.get("usgs") or {}
    built = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    narrative = _narrative(gipp, g7, g30, largest[0] if largest else {}, hotspot_summary, ga, dea)
    return {
        "built_at": built,
        "catalog": catalog,
        "kpis": {
            "gippsland_7d": g7,
            "gippsland_30d": g30,
            "gippsland_swarm": gipp,
            "largest": largest[0] if largest else {},
            "hotspots": hotspot_summary,
            "ga": ga,
            "usgs": usgs,
            "dea": dea,
        },
        "tones": {
            "g7": _tone_status(g7.get("status")),
            "g30": _tone_status(g30.get("status")),
            "swarm": _tone_status(gipp.get("status")),
            "ga": _tone_minutes(
                ga.get("p50_minutes"),
                SLA["ga_publication_minutes"]["green"],
                SLA["ga_publication_minutes"]["amber"],
            ),
            "dea": _tone_minutes(
                dea.get("p50_minutes"),
                SLA["hotspot_processing_minutes"]["green"],
                SLA["hotspot_processing_minutes"]["amber"],
            ),
            "hot_fresh": _tone_minutes(
                (dea.get("freshness_minutes") or 0) / 60 if dea.get("freshness_minutes") is not None else None,
                SLA["hotspot_freshness_hours"]["green"],
                SLA["hotspot_freshness_hours"]["amber"],
            ),
        },
        "weekly": weekly,
        "weekly_baseline": baseline_week[0] if baseline_week else None,
        "magnitude": mag,
        "states": states,
        "trend": trend,
        "swarm_events": swarm_events,
        "swarm_table": swarm,
        "latency": latency,
        "quake_hexes": quake_hex,
        "hot_hexes": hot_hex,
        "quake_points": quake_points,
        "narrative": narrative,
        "sla": SLA,
    }


def _fmt(value, digits=1):
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _narrative(gipp: dict, g7: dict, g30: dict, largest: dict, hot: dict, ga: dict, dea: dict) -> dict:
    status = gipp.get("status") or "unknown"
    words = {
        "unusual": "sits at or above the 99th percentile of equal-length windows since 2006",
        "elevated": "sits at or above the 95th percentile of equal-length windows since 2006",
        "typical": "is inside the usual range of equal-length windows since 2006",
        "insufficient_baseline": "does not have a long enough baseline to rank",
    }.get(status, "is described against the historical windows in the table")
    b_note = (
        f"The ML b-value for this window is {_fmt(gipp.get('b_value'), 2)} "
        f"(n={gipp.get('b_n')}, Mc={_fmt(gipp.get('mc'), 1)}). "
    )
    if gipp.get("b_stable"):
        b_note += (
            "The sample is large enough for a rough estimate, and it is still only a description of these magnitudes."
        )
    else:
        b_note += "Fewer than 50 events sit at or above Mc, so the b-value is too unstable to lean on. The count comparison is the clearer signal."
    place = largest.get("place") or "the catalogue"
    mag = largest.get("magnitude")
    when = largest.get("origin_time") or ""
    return {
        "headline": f"South Gippsland, M≥2 since 26 Sep 2026: {gipp.get('event_count_m2', '—')} events, status {status}.",
        "points": [
            f"In the South Gippsland box, the window from 26 Sep 2026 to the newest catalogue event has {gipp.get('event_count_m2', '—')} earthquakes of magnitude 2 or more ({gipp.get('event_count_all', '—')} of any magnitude). That count {words}. The median historical window of the same length has {_fmt(gipp.get('baseline_median'), 1)} events (95th percentile {_fmt(gipp.get('baseline_p95'), 1)}).",
            f"The last 7 days in that box: {g7.get('event_count_m2', '—')} events M≥2 ({g7.get('status', 'n/a')}). The last 30 days: {g30.get('event_count_m2', '—')} ({g30.get('status', 'n/a')}).",
            f"Largest catalogue event in the last 30 days of the whole Australian set: M{_fmt(mag, 1)} {largest.get('mag_type') or ''} — {place} at {when} UTC.",
            b_note,
            f"The public DEA hotspot feed is a rolling window of about 72 hours, not a seasonal archive. This snapshot has {hot.get('detections_72h', '—')} detections, {hot.get('detections_6h', '—')} in the newest 6 hours ({hot.get('cells_6h', '—')} distinct H3 resolution-6 cells). {hot.get('near_population', '—')} are within 25 km of a Natural Earth place of 10,000 people or more, and {hot.get('near_airport', '—')} are within 20 km of a medium or large airport. Detections within 25 km of Perth Airport (YPPH): {hot.get('near_perth_airport', '—')}.",
            f"Geoscience Australia median publication lag over the last 30 days (creation time minus origin, lags under 7 days) is {_fmt(ga.get('p50_minutes'), 0)} minutes. DEA median processing lag is {_fmt(dea.get('p50_minutes'), 0)} minutes. These are catalogue timestamps, not a promise about the next alert.",
        ],
    }


def build(database: Path = ANALYTICS_DB) -> Path:
    payload = build_payload(database)
    DASHBOARD_DIST.mkdir(parents=True, exist_ok=True)
    html = TEMPLATE.replace("/*__DATA__*/", "const DATA = " + json.dumps(payload, default=str) + ";")
    target = DASHBOARD_DIST / "index.html"
    target.write_text(html)
    (DASHBOARD_DIST / "payload.json").write_text(json.dumps(payload, default=str))
    return target


def main() -> None:
    path = build()
    print(path)


if __name__ == "__main__":
    main()
