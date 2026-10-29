"""Build the public dashboard from the dbt marts.

The page is for readers who are not data engineers. Statistics stay in the
README and INSIGHTS.md. Every number on the page is queried from a mart.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from aus_hazard.config import ANALYTICS_DB, DASHBOARD_DIST

TEMPLATE = (Path(__file__).resolve().parents[2] / "dashboard" / "template.html").read_text()

MELBOURNE = timedelta(hours=11)
ADELAIDE = timedelta(hours=10, minutes=30)
MONTHS = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    table = con.execute(sql).to_arrow_table()
    assert table is not None
    rows = table.to_pylist()
    for row in rows:
        for key, value in list(row.items()):
            if hasattr(value, "isoformat"):
                row[key] = value.isoformat()
    return rows


def _clock(moment: datetime, shift: timedelta) -> tuple[datetime, str]:
    local = moment + shift
    hour = local.hour
    suffix = "am" if hour < 12 else "pm"
    hour12 = hour % 12 or 12
    text = f"{local.day} {MONTHS[local.month - 1]} {local.year}, {hour12}:{local.minute:02d} {suffix}"
    return local, text


def _melbourne(moment: datetime) -> str:
    _, text = _clock(moment, MELBOURNE)
    return f"{text} Melbourne time"


def _adelaide(moment: datetime) -> str:
    _, text = _clock(moment, ADELAIDE)
    return f"{text} Adelaide time"


def _people(count: int | None) -> str | None:
    if count is None or count <= 0:
        return None
    if count >= 1000:
        rounded = int(round(count / 100.0) * 100)
        return f"About {rounded:,} people reported feeling it"
    if count >= 10:
        return f"{count:,} people reported feeling it"
    return "A few people reported feeling it"


def _level(status: str | None) -> str:
    return {"unusual": "high", "elevated": "elevated", "typical": "calm"}.get(status or "", "calm")


def _parse(value: str) -> datetime:
    text = value.replace("Z", "+00:00")
    return datetime.fromisoformat(text).replace(tzinfo=None)


def build_payload(database: Path = ANALYTICS_DB) -> dict[str, Any]:
    con = duckdb.connect(str(database), read_only=True)
    swarm = {row["scope"]: row for row in _rows(con, "select * from mart_swarm_activity")}
    gippsland = swarm["south_gippsland_30d"]
    australia = swarm["australia_7d"]
    updated = _parse(_rows(con, "select max(ingested_at) as ingested_at from fct_earthquake")[0]["ingested_at"])

    largest_rows = _rows(
        con,
        """
        select place, magnitude, origin_time, felt_reports, state_name, latitude, longitude
        from fct_earthquake
        where origin_time >= (select max(origin_time) from fct_earthquake) - interval 30 day
          and (
            state_name is not null
            or place ilike '%VIC%'
            or place ilike '%NSW%'
            or place ilike '%QLD%'
            or place ilike '%TAS%'
            or place ilike '%Australia%'
            or place ilike 'Offshore SA%'
            or place ilike 'Offshore Southern%'
            or place ilike 'Coastal%'
          )
        order by magnitude desc, felt_reports desc nulls last
        limit 1
        """,
    )
    largest = largest_rows[0]
    largest_time = _parse(largest["origin_time"])
    felt = _people(largest.get("felt_reports"))

    near = _rows(
        con,
        """
        with latest as (select max(observed_at) as t from fct_hotspot)
        select
            count(*) filter (
                where near_population and observed_at >= latest.t - interval 6 hour
            ) as near_recent,
            count(*) filter (where near_population) as near_window,
            count(*) as detections
        from fct_hotspot, latest
        """,
    )[0]

    years_raw = {
        int(row["y"]): int(row["n"])
        for row in _rows(
            con,
            """
            select year(origin_time) as y, count(*) as n
            from fct_earthquake
            where magnitude >= 2
              and latitude between -38.70 and -38.15
              and longitude between 145.55 and 146.30
            group by 1
            """,
        )
    }
    years = [{"year": year, "count": years_raw.get(year, 0)} for year in range(2006, 2027)]

    states = _rows(
        con,
        """
        select state_name as state, count(*) as n
        from fct_hotspot
        where state_name is not null
        group by 1
        order by n desc
        """,
    )
    offshore = _rows(
        con,
        "select count(*) as n from fct_hotspot where state_name is null",
    )[0]["n"]

    sequence = []
    for row in _rows(
        con,
        """
        select origin_time, magnitude, place, felt_reports
        from fct_earthquake
        where latitude between -38.70 and -38.15
          and longitude between 145.55 and 146.30
          and origin_time >= timestamp '2026-09-26'
        order by origin_time
        """,
    ):
        moment = _parse(row["origin_time"])
        sequence.append(
            {
                "time": row["origin_time"],
                "when": _melbourne(moment),
                "magnitude": row["magnitude"],
                "place": row["place"],
                "felt": _people(row.get("felt_reports")),
            }
        )

    quakes = []
    for row in _rows(
        con,
        """
        select longitude, latitude, magnitude, place, origin_time, felt_reports, state_name
        from fct_earthquake
        where magnitude >= 2
          and origin_time >= timestamp '2026-08-01'
          and (state_name is not null or latitude < -12)
          and longitude between 110 and 160
        order by magnitude
        """,
    ):
        moment = _parse(row["origin_time"])
        when = _melbourne(moment)
        place = row["place"] or "Offshore Australia"
        if place.startswith("Offshore SA"):
            when = f"{_adelaide(moment)} ({_melbourne(moment)})"
        quakes.append(
            {
                "lon": row["longitude"],
                "lat": row["latitude"],
                "magnitude": round(float(row["magnitude"]), 1),
                "place": place,
                "when": when,
                "felt": _people(row.get("felt_reports")),
            }
        )

    fires = []
    for row in _rows(
        con,
        """
        select
            avg(latitude) as lat,
            avg(longitude) as lon,
            count(*) as n,
            arg_max(
                nearest_place,
                case when near_population then coalesce(nearest_place_pop, 0) else -1 end
            ) as place,
            count(*) filter (where near_population) as near_towns
        from fct_hotspot
        group by h3_res4
        having count(*) >= 8
        order by n desc
        limit 700
        """,
    ):
        place = row["place"] if row["near_towns"] else None
        fires.append(
            {
                "lon": row["lon"],
                "lat": row["lat"],
                "n": int(row["n"]),
                "place": place,
            }
        )

    con.close()

    gippsland_n = int(gippsland["event_count_m2"])
    australia_n = int(australia["event_count_m2"])
    usual_week = int(round(float(australia["baseline_median"])))
    near_recent = int(near["near_recent"])
    near_window = int(near["near_window"])
    mag = round(float(largest["magnitude"]), 1)
    when = _melbourne(largest_time)

    return {
        "updated": _melbourne(updated).replace(" Melbourne time", "") + " Melbourne time",
        "headline": (f"South Gippsland has had {gippsland_n} earthquakes this month. A typical month there has none."),
        "dek": "The rest of Australia looks like a normal week. This page is not a warning.",
        "cards": [
            {
                "label": "Gippsland, this month",
                "value": str(gippsland_n),
                "unit": "earthquakes",
                "takeaway": "A typical month here has none.",
                "level": _level(gippsland["status"]),
                "level_label": {"high": "High", "elevated": "Elevated", "calm": "Calm"}[_level(gippsland["status"])],
            },
            {
                "label": "Australia, this week",
                "value": str(australia_n),
                "unit": "earthquakes",
                "takeaway": f"A usual week in and around Australia has about {usual_week}.",
                "level": _level(australia["status"]),
                "level_label": {"high": "High", "elevated": "Elevated", "calm": "Calm"}[_level(australia["status"])],
            },
            {
                "label": "Largest recent quake",
                "value": str(mag),
                "unit": "magnitude",
                "takeaway": f"{largest['place']}. {when}." + (f" {felt}" if felt else ""),
                "level": "elevated",
                "level_label": "Elevated",
            },
            {
                "label": "Heat near towns",
                "value": f"{near_recent:,}",
                "unit": "in the last few hours",
                "takeaway": (
                    f"{near_window:,} satellite detections were near towns over the last three days. "
                    "These can be fires, or something else that is hot."
                ),
                "level": "elevated",
                "level_label": "Elevated",
            },
        ],
        "charts": {
            "years": {
                "title": "Gippsland's earthquakes stand out against 20 years",
                "note": (
                    "Each bar is a year of earthquakes of magnitude 2 or more near Leongatha. "
                    f"{years[-1]['count']} have been recorded in 2026, and the year is not finished. "
                    "2009 and 2012 were busier still."
                ),
                "points": years,
            },
            "states": {
                "title": "On land, most of the heat is in the north",
                "note": (
                    "Satellite detections over the last three days. "
                    f"Another {int(offshore):,} were offshore or not tied to a state. "
                    "A detection is heat, not a confirmed bushfire."
                ),
                "points": states,
            },
            "sequence": {
                "title": "The shaking near Leongatha has come in bursts",
                "note": "Each dot is one earthquake since 26 September. Higher means a larger quake.",
                "points": sequence,
            },
        },
        "meaning": [
            "If you are in South Gippsland, more small earthquakes may be felt for a while. If the ground shakes, drop, cover, and hold on.",
            f"A magnitude {mag} quake near Leongatha on 3 October was widely felt. Feeling one earthquake does not mean a larger one is on the way.",
            "Orange areas are satellite heat. They can be a bushfire or a false alarm. For what to do, use the official sources below. This page is not a warning service.",
        ],
        "links": [
            {"name": "Geoscience Australia earthquakes", "href": "https://earthquakes.ga.gov.au/"},
            {"name": "VicEmergency", "href": "https://emergency.vic.gov.au/"},
            {
                "name": "How it's built",
                "href": "https://github.com/LuciferDataEngineer/aus-hazard-stream#how-its-built",
            },
        ],
        "map": {
            "gippsland": [145.89, -38.42],
            "australia": [134.2, -28.0],
            "quakes": quakes,
            "fires": fires,
        },
    }


def build(database: Path = ANALYTICS_DB) -> Path:
    payload = build_payload(database)
    DASHBOARD_DIST.mkdir(parents=True, exist_ok=True)
    html = TEMPLATE.replace("/*__DATA__*/", "const DATA = " + json.dumps(payload) + ";")
    target = DASHBOARD_DIST / "index.html"
    target.write_text(html)
    (DASHBOARD_DIST / "payload.json").write_text(json.dumps(payload))
    return target


def main() -> None:
    print(build())


if __name__ == "__main__":
    main()
