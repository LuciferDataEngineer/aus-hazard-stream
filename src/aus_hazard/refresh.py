"""Pull the latest free feeds and rebuild marts. Exits 0 when feeds are down.

Used by the scheduled GitHub Action. It does not commit data. A failed
download leaves the previous snapshot in place and skips the rebuild.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pyarrow.parquet as pq

from aus_hazard.config import DATA, SNAPSHOTS
from aus_hazard.fetch import _client, _write_parquet, fetch_ga, fetch_hotspots, fetch_usgs


def _merge(existing_path, fresh_rows, id_field: str) -> list[dict]:
    current = pq.read_table(existing_path).to_pylist() if existing_path.exists() else []
    by_id = {row[id_field]: row for row in current}
    for row in fresh_rows:
        previous = by_id.get(row[id_field])
        if previous is None or int(row.get("version") or 0) >= int(previous.get("version") or 0):
            by_id[row[id_field]] = row
    return list(by_id.values())


def try_refresh() -> bool:
    ingested = datetime.now(UTC)
    start = (ingested - timedelta(days=30)).strftime("%Y-%m-%d")
    status_path = DATA / "refresh_status.json"
    try:
        end = (ingested + timedelta(days=1)).strftime("%Y-%m-%d")
        ga_cql = f"located_in_australia='Y' AND origin_time >= '{start}T00:00:00Z' AND origin_time < '{end}T00:00:00Z'"
        with _client() as client:
            recent_usgs, _ = fetch_usgs(client, ingested, min_magnitude=1.0, start=start, end=end)
            recent_ga, _ = fetch_ga(client, ingested, cql=ga_cql)
            hot_rows, _ = fetch_hotspots(client, ingested)
    except Exception as exc:
        status_path.write_text(
            json.dumps(
                {
                    "ok": False,
                    "refreshed_at": ingested.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "error": str(exc),
                },
                indent=2,
            )
        )
        print(f"feeds unavailable, exiting cleanly: {exc}")
        return False
    merged_usgs = _merge(SNAPSHOTS / "usgs_earthquakes.parquet", recent_usgs, "event_id")
    merged_ga = _merge(SNAPSHOTS / "ga_earthquakes.parquet", recent_ga, "event_id")
    _write_parquet(SNAPSHOTS / "usgs_earthquakes.parquet", merged_usgs)
    _write_parquet(SNAPSHOTS / "ga_earthquakes.parquet", merged_ga)
    _write_parquet(SNAPSHOTS / "dea_hotspots.parquet", hot_rows)
    status_path.write_text(
        json.dumps(
            {
                "ok": True,
                "refreshed_at": ingested.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "usgs_rows": len(merged_usgs),
                "ga_rows": len(merged_ga),
                "hotspot_rows": len(hot_rows),
            },
            indent=2,
        )
    )
    return True


def main() -> None:
    if not try_refresh():
        return
    from aus_hazard.dashboard import build
    from aus_hazard.pipeline import replay
    from aus_hazard.publish import export_marts, run_dbt

    replay()
    run_dbt()
    export_marts()
    build()


if __name__ == "__main__":
    main()
