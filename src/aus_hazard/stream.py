"""Live poll of the public feeds, published to Redpanda when it is up.

``--once`` does a single pass. Without ``REDPANDA_BROKERS`` the same producer
and consumer run on the in-memory bus, which is how CI exercises the path.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from datetime import UTC, datetime, timedelta

from aus_hazard.bus import InMemoryBus, publish_kafka
from aus_hazard.config import TOPICS, USER_AGENT
from aus_hazard.fetch import DEA_WFS, GA_WFS, USGS_QUERY, _client
from aus_hazard.normalize import hotspot_from_dea, quake_from_ga, quake_from_usgs
from aus_hazard.pipeline import consume, produce


def poll_once() -> dict[str, int | str]:
    ingested = datetime.now(UTC)
    start = (ingested - timedelta(days=7)).strftime("%Y-%m-%d")
    end = (ingested + timedelta(days=1)).strftime("%Y-%m-%d")
    ga_cql = (
        "located_in_australia='Y' AND preferred_magnitude >= 0 "
        f"AND origin_time >= '{start}T00:00:00Z' AND origin_time < '{end}T00:00:00Z'"
    )
    quakes = []
    hotspots = []
    with _client() as client:
        usgs = client.get(
            USGS_QUERY,
            params={
                "format": "geojson",
                "starttime": start,
                "endtime": end,
                "minlatitude": -45,
                "maxlatitude": -9,
                "minlongitude": 110,
                "maxlongitude": 160,
                "minmagnitude": 1,
                "orderby": "time",
            },
        )
        usgs.raise_for_status()
        for feature in usgs.json().get("features") or []:
            row = quake_from_usgs(feature, ingested)
            if row and row["event_type"] == "earthquake":
                quakes.append(row)
        ga = client.get(
            GA_WFS,
            params={
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeNames": "earthquakes:earthquakes",
                "outputFormat": "application/json",
                "count": "500",
                "CQL_FILTER": ga_cql,
                "sortBy": "origin_time D",
            },
        )
        ga.raise_for_status()
        for feature in ga.json().get("features") or []:
            row = quake_from_ga(feature, ingested)
            if row:
                quakes.append(row)
        # Last six hours only, so a live poll does not pull the whole 72-hour layer.
        since = (ingested - timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")
        hot = client.get(
            DEA_WFS,
            params={
                "service": "WFS",
                "version": "1.0.0",
                "request": "GetFeature",
                "typeName": "public:hotspots_three_days",
                "outputFormat": "application/json",
                "maxFeatures": "2000",
                "CQL_FILTER": f"datetime >= '{since}'",
            },
        )
        hot.raise_for_status()
        payload = hot.json()
        for feature in payload.get("features") or []:
            row = hotspot_from_dea(feature, ingested)
            if row:
                hotspots.append(row)
    brokers = os.environ.get("REDPANDA_BROKERS", "").strip()
    if brokers:
        from aus_hazard.bus import Message, register_redpanda_schemas

        registry = os.environ.get("REDPANDA_REGISTRY", "http://localhost:18081")
        register_redpanda_schemas(registry)
        messages = [Message(TOPICS["quakes"], row["event_id"], row) for row in quakes] + [
            Message(TOPICS["hotspots"], row["hotspot_id"], row) for row in hotspots
        ]
        asyncio.run(publish_kafka(brokers, messages))
        return {"quakes": len(quakes), "hotspots": len(hotspots), "bus": "redpanda"}
    bus = InMemoryBus()
    produce(bus, quakes, TOPICS["quakes"], "event_id", "quake")
    produce(bus, hotspots, TOPICS["hotspots"], "hotspot_id", "hotspot")
    consumed = consume(bus)
    return {
        "quakes": len(consumed["ga"]) + len(consumed["usgs"]),
        "hotspots": len(consumed["hotspots"]),
        "dlq": len(consumed["dlq"]),
        "bus": "memory",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Poll hazard feeds")
    parser.add_argument("--once", action="store_true", help="Single poll, then exit")
    parser.add_argument("--interval", type=int, default=300, help="Seconds between polls")
    args = parser.parse_args()
    print(f"user-agent {USER_AGENT}")
    if args.once or not os.environ.get("REDPANDA_BROKERS"):
        print(poll_once())
        if not args.once and not os.environ.get("REDPANDA_BROKERS"):
            print(
                "Set REDPANDA_BROKERS and run `make up` for a continuous consumer. One poll was executed on the in-memory bus."
            )
        return
    while True:
        try:
            print(poll_once(), flush=True)
        except Exception as exc:
            print(f"poll failed: {exc}", flush=True)
        import time

        time.sleep(args.interval)


if __name__ == "__main__":
    main()
