"""H3 indexes and spatial joins onto states, towns, and airports."""

from __future__ import annotations

import json
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from aus_hazard.config import NEAR_AIRPORT_KM, NEAR_PLACE_KM, PLACE_POP_MIN, REFERENCE
from aus_hazard.geo import h3_cells


def attach_h3(rows: list[dict[str, Any]], lat_key: str = "latitude", lon_key: str = "longitude") -> None:
    for row in rows:
        cells = h3_cells(float(row[lat_key]), float(row[lon_key]))
        row.update(cells)


def _records_from_table(table: pa.Table) -> list[dict[str, Any]]:
    return table.to_pylist()


def spatial_enrich(
    quakes: list[dict[str, Any]],
    hotspots: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Point-in-polygon to states, nearest populated place, nearest airport.

    Distances use DuckDB's spheroid (metres). A hotspot is 'near population'
    when the nearest Natural Earth place has pop_max >= 10,000 and is within
    25 km, and 'near infrastructure' when a medium or large airport is within
    20 km. Those thresholds are operational rules of thumb, not a risk model.
    """
    states = REFERENCE / "au_states.geojson"
    places = REFERENCE / "populated_places.parquet"
    airports = REFERENCE / "airports.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    # DuckDB 2.0 rejects spheroid distances until axis order is explicit.
    # GeoJSON and ST_Point in this project are longitude, latitude.
    con.execute("SET geometry_always_xy = true;")
    con.execute("INSTALL h3 FROM community; LOAD h3;")

    con.register(
        "quakes",
        pa.Table.from_pylist(quakes) if quakes else pa.table({"catalog_id": pa.array([], type=pa.string())}),
    )
    hot_cols = [
        "hotspot_id",
        "latitude",
        "longitude",
        "state",
        "observed_at",
        "confidence",
        "power",
        "temp_kelvin",
        "satellite",
        "sensor",
        "algorithm",
        "processed_at",
        "loaded_at",
        "version",
        "ingested_at",
        "h3_res4",
        "h3_res5",
        "h3_res6",
        "source",
        "source_event_id",
        "schema_version",
    ]
    slim = [{key: row.get(key) for key in hot_cols} for row in hotspots]
    con.register(
        "hotspots",
        pa.Table.from_pylist(slim) if slim else pa.table({"hotspot_id": pa.array([], type=pa.string())}),
    )
    con.execute(f"CREATE TABLE states AS SELECT * FROM st_read('{states}')")
    con.execute(f"CREATE TABLE places AS SELECT * FROM read_parquet('{places}')")
    con.execute(f"CREATE TABLE airports AS SELECT * FROM read_parquet('{airports}')")

    # A point on a simplified border can miss. Leave the state null rather than guess.
    # A point on a shared border can sit in two simplified polygons.
    # Keep one state so the fact table stays one row per event.
    enriched_quakes = con.execute(
        """
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT q.*, s.state_name AS joined_state,
                   row_number() OVER (PARTITION BY q.catalog_id ORDER BY s.state_name) AS rn
            FROM quakes q
            LEFT JOIN states s
              ON ST_Contains(s.geom, ST_Point(q.longitude, q.latitude))
        )
        WHERE rn = 1
        """
    ).to_arrow_table()

    # One-degree prefilter (~111 km). The near-population flag is 25 km, so a
    # town further away cannot flip it. This avoids a full cartesian product.
    con.execute(
        f"""
        CREATE TABLE hotspot_place AS
        SELECT hotspot_id, name AS nearest_place, pop_max AS nearest_place_pop, km AS nearest_place_km
        FROM (
            SELECT h.hotspot_id,
                   p.name,
                   p.pop_max,
                   ST_Distance_Spheroid(
                       ST_Point(h.longitude, h.latitude),
                       ST_Point(p.longitude, p.latitude)
                   ) / 1000.0 AS km,
                   row_number() OVER (PARTITION BY h.hotspot_id ORDER BY km) AS rn
            FROM hotspots h
            JOIN places p
              ON p.pop_max >= {int(PLACE_POP_MIN)}
             AND abs(p.latitude - h.latitude) <= 1.0
             AND abs(p.longitude - h.longitude) <= 1.0
        )
        WHERE rn = 1
        """
    )
    con.execute(
        """
        CREATE TABLE hotspot_airport AS
        SELECT hotspot_id, name AS nearest_airport, ident AS nearest_airport_ident, km AS nearest_airport_km
        FROM (
            SELECT h.hotspot_id,
                   a.name,
                   a.ident,
                   ST_Distance_Spheroid(
                       ST_Point(h.longitude, h.latitude),
                       ST_Point(a.longitude, a.latitude)
                   ) / 1000.0 AS km,
                   row_number() OVER (PARTITION BY h.hotspot_id ORDER BY km) AS rn
            FROM hotspots h
            JOIN airports a
              ON abs(a.latitude - h.latitude) <= 1.0
             AND abs(a.longitude - h.longitude) <= 1.0
        )
        WHERE rn = 1
        """
    )
    enriched_hot = con.execute(
        """
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT h.*,
                   s.state_name AS joined_state,
                   hp.nearest_place,
                   hp.nearest_place_pop,
                   hp.nearest_place_km,
                   ha.nearest_airport,
                   ha.nearest_airport_ident,
                   ha.nearest_airport_km,
                   row_number() OVER (PARTITION BY h.hotspot_id ORDER BY s.state_name) AS rn
            FROM hotspots h
            LEFT JOIN states s
              ON ST_Contains(s.geom, ST_Point(h.longitude, h.latitude))
            LEFT JOIN hotspot_place hp USING (hotspot_id)
            LEFT JOIN hotspot_airport ha USING (hotspot_id)
        )
        WHERE rn = 1
        """
    ).to_arrow_table()

    # Confirm DuckDB H3 and h3-py agree on one point (stored for the lineage note).
    h3_check = None
    if quakes:
        sample = quakes[0]
        h3_check = con.execute(
            "SELECT h3_latlng_to_cell(?, ?, 4)",
            [float(sample["latitude"]), float(sample["longitude"])],
        ).fetchone()

    assert enriched_quakes is not None
    quake_out = enriched_quakes.to_pylist()
    for row in quake_out:
        row["state_name"] = row.pop("joined_state", None) or row.get("state_name")
    assert enriched_hot is not None
    hot_out = enriched_hot.to_pylist()
    for row in hot_out:
        feed_state = row.get("state")
        joined = row.pop("joined_state", None)
        row["state_name"] = feed_state or joined
        place_km = row.get("nearest_place_km")
        pop = row.get("nearest_place_pop") or 0
        row["near_population"] = bool(place_km is not None and place_km <= NEAR_PLACE_KM and pop >= PLACE_POP_MIN)
        airport_km = row.get("nearest_airport_km")
        row["near_airport"] = bool(airport_km is not None and airport_km <= NEAR_AIRPORT_KM)
    con.close()
    if h3_check is not None:
        (REFERENCE / "h3_duckdb_check.json").write_text(
            json.dumps({"duckdb_h3_int": int(h3_check[0]), "h3_py": quakes[0].get("h3_res4")})
        )
    return quake_out, hot_out


def write_silver(quakes: list[dict[str, Any]], hotspots: list[dict[str, Any]], directory: Any) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(quakes), directory / "quakes.parquet", compression="zstd")
    pq.write_table(pa.Table.from_pylist(hotspots), directory / "hotspots.parquet", compression="zstd")
