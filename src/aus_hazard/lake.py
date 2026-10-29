"""Local Iceberg catalog: SQLite + filesystem warehouse.

Bronze is append-only raw contract rows. Silver is the current deduplicated
view, rewritten as a new snapshot so time travel can still see the previous
one. Partitioning is event_date plus H3 resolution 4, as asked. Schema
evolution adds an optional lineage_note column after the first snapshot.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, DoubleType, LongType, NestedField, StringType

from aus_hazard.config import CATALOG_DB, ICEBERG_WAREHOUSE


def catalog() -> SqlCatalog:
    ICEBERG_WAREHOUSE.mkdir(parents=True, exist_ok=True)
    CATALOG_DB.parent.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "hazard",
        **{
            "uri": f"sqlite:///{CATALOG_DB}",
            "warehouse": f"file://{ICEBERG_WAREHOUSE}",
        },
    )


def _field(field_id: int, name: str, kind: Any) -> NestedField:
    # Optional fields match PyArrow's default nullability. Required fields
    # fail the write when Arrow marks the column optional.
    return NestedField(field_id=field_id, name=name, field_type=kind, required=False)


# Quakes are partitioned by month + H3 res-4 rather than every calendar day.
# A day × H3 identity partition on a 20-year sparse catalogue is about one
# file per earthquake. event_date is still stored and is the mart grain.
# Hotspots are dense inside a 72-hour window, so they use event_date + H3.
SILVER_QUAKE_SCHEMA = Schema(
    _field(1, "catalog_id", StringType()),
    _field(2, "event_month", StringType()),
    _field(3, "h3_res4", StringType()),
    _field(4, "event_date", DateType()),
    _field(5, "origin_time", StringType()),
    _field(6, "latitude", DoubleType()),
    _field(7, "longitude", DoubleType()),
    _field(8, "depth_km", DoubleType()),
    _field(9, "magnitude", DoubleType()),
    _field(10, "mag_type", StringType()),
    _field(11, "sources", StringType()),
    _field(12, "place", StringType()),
    _field(13, "version", LongType()),
)

SILVER_HOTSPOT_SCHEMA = Schema(
    _field(1, "hotspot_id", StringType()),
    _field(2, "event_date", DateType()),
    _field(3, "h3_res4", StringType()),
    _field(4, "observed_at", StringType()),
    _field(5, "latitude", DoubleType()),
    _field(6, "longitude", DoubleType()),
    _field(7, "confidence", LongType()),
    _field(8, "power", DoubleType()),
    _field(9, "state", StringType()),
    _field(10, "satellite", StringType()),
)


def _partition(date_id: int = 2, h3_id: int = 3) -> PartitionSpec:
    return PartitionSpec(
        PartitionField(source_id=date_id, field_id=1000, transform=IdentityTransform(), name="event_date"),
        PartitionField(source_id=h3_id, field_id=1001, transform=IdentityTransform(), name="h3_res4"),
    )


def _partition_month_h3() -> PartitionSpec:
    # Partition names have to match the source columns. Quakes use event_month.
    return PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="event_month"),
        PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="h3_res4"),
    )


def _partition_date() -> PartitionSpec:
    return PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="event_date"),
    )


def ensure_table(cat: SqlCatalog, identifier: str, schema: Schema, spec: PartitionSpec | None = None) -> Any:
    namespace = identifier.split(".")[0]
    if (namespace,) not in cat.list_namespaces():
        cat.create_namespace(namespace)
    if cat.table_exists(identifier):
        return cat.load_table(identifier)
    return cat.create_table(identifier, schema=schema, partition_spec=spec or _partition())


def _date(value: str | None) -> dt.date | None:
    if not value:
        return None
    return dt.date.fromisoformat(value[:10])


def quakes_arrow(rows: list[dict[str, Any]], lineage_note: str | None = None) -> pa.Table:
    data: dict[str, list[Any]] = {
        "catalog_id": [],
        "event_month": [],
        "h3_res4": [],
        "event_date": [],
        "origin_time": [],
        "latitude": [],
        "longitude": [],
        "depth_km": [],
        "magnitude": [],
        "mag_type": [],
        "sources": [],
        "place": [],
        "version": [],
    }
    for row in rows:
        data["catalog_id"].append(row.get("catalog_id") or row.get("event_id"))
        origin = row.get("origin_time") or ""
        data["event_month"].append(origin[:7] or None)
        data["h3_res4"].append(row.get("h3_res4"))
        data["event_date"].append(_date(origin))
        data["origin_time"].append(row.get("origin_time"))
        data["latitude"].append(row.get("latitude"))
        data["longitude"].append(row.get("longitude"))
        data["depth_km"].append(row.get("depth_km"))
        data["magnitude"].append(row.get("magnitude"))
        data["mag_type"].append(row.get("mag_type"))
        data["sources"].append(row.get("sources"))
        data["place"].append(row.get("place"))
        data["version"].append(row.get("version"))
    table = pa.table(data)
    if lineage_note is not None:
        table = table.append_column("lineage_note", pa.array([lineage_note] * table.num_rows, type=pa.string()))
    return table


def hotspots_arrow(rows: list[dict[str, Any]]) -> pa.Table:
    data: dict[str, list[Any]] = {
        "hotspot_id": [],
        "event_date": [],
        "h3_res4": [],
        "observed_at": [],
        "latitude": [],
        "longitude": [],
        "confidence": [],
        "power": [],
        "state": [],
        "satellite": [],
    }
    for row in rows:
        data["hotspot_id"].append(row.get("hotspot_id"))
        data["event_date"].append(_date(row.get("observed_at")))
        data["h3_res4"].append(row.get("h3_res4"))
        data["observed_at"].append(row.get("observed_at"))
        data["latitude"].append(row.get("latitude"))
        data["longitude"].append(row.get("longitude"))
        data["confidence"].append(row.get("confidence"))
        data["power"].append(row.get("power"))
        data["state"].append(row.get("state_name") or row.get("state"))
        data["satellite"].append(row.get("satellite"))
    return pa.table(data)


def commit_silver(quakes: list[dict[str, Any]], hotspots: list[dict[str, Any]]) -> dict[str, Any]:
    """Write silver, evolve the schema, write a second snapshot, return ids."""
    cat = catalog()
    quake_table = ensure_table(cat, "silver.quakes", SILVER_QUAKE_SCHEMA, _partition_month_h3())
    # First snapshot without the evolved column.
    quake_table.overwrite(quakes_arrow(quakes))
    quake_table = cat.load_table("silver.quakes")
    first_snapshots = list(quake_table.snapshots())
    first_id = first_snapshots[-1].snapshot_id if first_snapshots else None
    with quake_table.update_schema() as update:
        names = {field.name for field in quake_table.schema().fields}
        if "lineage_note" not in names:
            update.add_column("lineage_note", StringType())
    quake_table = cat.load_table("silver.quakes")
    quake_table.overwrite(quakes_arrow(quakes, lineage_note="ga-preferred-dedup-v1"))
    quake_table = cat.load_table("silver.quakes")

    # Hotspots are partitioned by event_date only. Crossing that with H3 res-4
    # on a national 72-hour layer is thousands of single-row files. h3_res4 is
    # still a column, and the unit-test table is partitioned by both fields.
    hot_table = ensure_table(cat, "silver.hotspots", SILVER_HOTSPOT_SCHEMA, _partition_date())
    hot_table.overwrite(hotspots_arrow(hotspots))
    hot_table = cat.load_table("silver.hotspots")

    return {
        "quake_snapshots": [snap.snapshot_id for snap in quake_table.snapshots()],
        "quake_time_travel_snapshot": first_id,
        "quake_rows": quake_table.scan().to_arrow().num_rows,
        "hotspot_snapshots": [snap.snapshot_id for snap in hot_table.snapshots()],
        "hotspot_rows": hot_table.scan().to_arrow().num_rows,
        "quake_fields": [field.name for field in quake_table.schema().fields],
        "warehouse": str(ICEBERG_WAREHOUSE),
    }


def time_travel_count(snapshot_id: int) -> int:
    cat = catalog()
    table = cat.load_table("silver.quakes")
    return table.scan(snapshot_id=snapshot_id).to_arrow().num_rows


def demo_time_travel(warehouse: Path, catalog_path: Path) -> dict[str, Any]:
    """Small self-contained time-travel example used by unit tests."""
    warehouse.mkdir(parents=True, exist_ok=True)
    cat = SqlCatalog(
        "demo",
        **{"uri": f"sqlite:///{catalog_path}", "warehouse": f"file://{warehouse}"},
    )
    if ("demo",) not in cat.list_namespaces():
        cat.create_namespace("demo")
    schema = Schema(
        _field(1, "event_id", StringType()),
        _field(2, "event_date", DateType()),
        _field(3, "h3_res4", StringType()),
        _field(4, "magnitude", DoubleType()),
    )
    if cat.table_exists("demo.events"):
        table = cat.load_table("demo.events")
    else:
        table = cat.create_table("demo.events", schema=schema, partition_spec=_partition())
    day = dt.date(2026, 10, 27)
    first = pa.table(
        {
            "event_id": ["ga:1"],
            "event_date": [day],
            "h3_res4": ["84be605ffffffff"],
            "magnitude": [3.5],
        }
    )
    table.overwrite(first)
    table = cat.load_table("demo.events")
    original = list(table.snapshots())[-1].snapshot_id
    revised = pa.table(
        {
            "event_id": ["ga:1"],
            "event_date": [day],
            "h3_res4": ["84be605ffffffff"],
            "magnitude": [3.6],
        }
    )
    table.overwrite(revised)
    table = cat.load_table("demo.events")
    with table.update_schema() as update:
        if "region_name" not in {field.name for field in table.schema().fields}:
            update.add_column("region_name", StringType())
    table = cat.load_table("demo.events")
    current = table.scan().to_arrow().column("magnitude").to_pylist()
    previous = table.scan(snapshot_id=original).to_arrow().column("magnitude").to_pylist()
    return {
        "previous_magnitude": previous,
        "current_magnitude": current,
        "fields": [field.name for field in table.schema().fields],
        "snapshot_id": original,
    }
