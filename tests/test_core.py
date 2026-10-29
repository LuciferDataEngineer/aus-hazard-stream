"""Unit tests for contracts, matching, watermarks, and the b-value."""

from __future__ import annotations

import json
import math
import random
from datetime import UTC, datetime, timedelta

from aus_hazard.bus import InMemoryBus
from aus_hazard.config import MANIFEST
from aus_hazard.contracts import validate_hotspot, validate_quake
from aus_hazard.dedup import unify_catalogs, upsert_by_version
from aus_hazard.geo import haversine_km
from aus_hazard.normalize import quake_from_usgs
from aus_hazard.pipeline import consume, produce
from aus_hazard.stats import gutenberg_richter_b, maximum_curvature_mc, st_dbscan
from aus_hazard.timeutil import iso
from aus_hazard.windows import Watermark


def _quake(**overrides):
    row = {
        "schema_version": 1,
        "event_id": "ga:1",
        "source": "ga",
        "source_event_id": "1",
        "origin_time": "2026-10-27T12:52:52.818Z",
        "updated_time": "2026-10-27T13:10:00.000Z",
        "created_time": "2026-10-27T13:05:00.000Z",
        "latitude": -38.425,
        "longitude": 145.867,
        "depth_km": 8.0,
        "magnitude": 3.5,
        "mag_type": "MLa075",
        "place": "8 km NW of Leongatha, VIC",
        "status": "FINL",
        "event_type": "earthquake",
        "version": 1,
        "felt_reports": 0,
        "url": None,
        "ingested_at": "2026-10-29T10:36:13.000Z",
    }
    row.update(overrides)
    return row


def test_contract_accepts_a_complete_quake():
    assert validate_quake(_quake()) == []


def test_contract_dead_letters_a_missing_magnitude():
    row = _quake()
    del row["magnitude"]
    assert validate_quake(row)


def test_v2_optional_quality_flag():
    row = _quake(schema_version=2, quality_flag="reviewed")
    assert validate_quake(row) == []


def test_hotspot_confidence_bounds():
    row = {
        "schema_version": 1,
        "hotspot_id": "dea:1",
        "source": "dea",
        "source_event_id": "1",
        "observed_at": "2026-10-29T04:18:52.000Z",
        "latitude": -31.97,
        "longitude": 116.06,
        "temp_kelvin": 320.0,
        "power": 12.0,
        "confidence": 80,
        "state": "WA",
        "satellite": "HIMAWARI-9",
        "sensor": "AHI",
        "algorithm": "BRIGHT AHI",
        "processed_at": "2026-10-29T04:29:34.000Z",
        "loaded_at": "2026-10-29T04:29:42.000Z",
        "version": 1,
        "ingested_at": "2026-10-29T10:36:13.000Z",
    }
    assert validate_hotspot(row) == []
    row["confidence"] = 140
    assert validate_hotspot(row)


def test_version_upsert_keeps_the_newer_solution():
    table = {}
    assert upsert_by_version(table, _quake(version=1, magnitude=3.4), "event_id") == "insert"
    assert upsert_by_version(table, _quake(version=3, magnitude=3.5), "event_id") == "update"
    assert upsert_by_version(table, _quake(version=2, magnitude=9.0), "event_id") == "stale"
    assert table["ga:1"]["magnitude"] == 3.5


def test_ga_and_usgs_match_within_tolerance():
    ga = _quake()
    usgs = _quake(
        event_id="usgs:us1",
        source="usgs",
        source_event_id="us1",
        origin_time="2026-10-27T12:53:20.000Z",
        latitude=-38.46,
        longitude=145.90,
        magnitude=3.7,
        mag_type="mb",
        version=100,
        url="https://earthquake.usgs.gov/earthquakes/eventpage/us1",
    )
    unified = unify_catalogs([ga], [usgs])
    assert len(unified) == 1
    assert unified[0]["sources"] == "ga+usgs"
    assert unified[0]["ga_id"] == "1"
    assert unified[0]["usgs_id"] == "us1"
    assert unified[0]["magnitude"] == 3.5


def test_distant_events_stay_separate():
    ga = _quake()
    usgs = _quake(
        event_id="usgs:far",
        source="usgs",
        source_event_id="far",
        latitude=-20.0,
        longitude=130.0,
        origin_time="2026-10-27T12:52:52.000Z",
    )
    assert len(unify_catalogs([ga], [usgs])) == 2


def test_watermark_flags_a_late_hour():
    wm = Watermark(lateness_ms=60 * 60 * 1000)
    hour = 3_600_000
    assert wm.observe(0) == "on_time"
    assert wm.observe(3 * hour) == "on_time"
    # The hour starting at 0 closed once the watermark passed its end.
    assert wm.observe(10 * 60 * 1000) == "late"
    assert wm.late_events == 1


def test_haversine_one_degree_latitude():
    distance = haversine_km(-38.0, 145.0, -37.0, 145.0)
    assert 110 < distance < 112


def test_b_value_recovers_a_synthetic_catalogue():
    rng = random.Random(7)
    b_true = 1.0
    mc = 2.0
    magnitudes = []
    for _ in range(400):
        # Inverse CDF of the exponential GR distribution above Mc.
        uniform = min(rng.random(), 0.999)
        magnitudes.append(mc - math.log10(1 - uniform) / b_true)
    estimate, n = gutenberg_richter_b(magnitudes, mc)
    assert n == 400
    assert estimate is not None
    assert abs(estimate - b_true) < 0.15
    assert maximum_curvature_mc(magnitudes) is not None


def test_stdbscan_finds_two_clusters():
    points = []
    for index in range(6):
        points.append((f"a{index}", -38.40, 145.90, 100 + index))
    for index in range(6):
        points.append((f"b{index}", -34.90, 138.60, 400 + index))
    points.append(("noise", -20.0, 130.0, 100))
    result = st_dbscan(points, eps_km=30, eps_hours=48, min_samples=4)
    assert result.n_clusters == 2
    assert result.labels["noise"] == -1
    assert result.labels["a0"] == result.labels["a5"]
    assert result.labels["a0"] != result.labels["b0"]


def test_replay_bus_dead_letters_bad_rows():
    bus = InMemoryBus()
    good = _quake()
    bad = _quake(event_id="ga:bad", source_event_id="bad")
    del bad["magnitude"]
    published = produce(bus, [good, bad], "quakes.raw", "event_id", "quake")
    assert published == 1
    consumed = consume(bus)
    assert len(consumed["ga"]) == 1
    assert len(consumed["dlq"]) == 1


def test_usgs_feature_normalises():
    feature = {
        "id": "us7000test",
        "properties": {
            "mag": 4.2,
            "place": "offshore",
            "time": 1790000000000,
            "updated": 1790001000000,
            "magType": "mb",
            "type": "earthquake",
            "status": "reviewed",
            "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us7000test",
        },
        "geometry": {"coordinates": [138.6, -36.4, 10]},
    }
    row = quake_from_usgs(feature, datetime(2026, 10, 29, tzinfo=UTC))
    assert row is not None
    assert row["event_id"] == "usgs:us7000test"
    assert validate_quake(row) == []
    assert row["origin_time"].endswith("Z")


def test_manifest_hashes_match_files():
    manifest = json.loads(MANIFEST.read_text())
    assert manifest["errors"] == []
    for entry in manifest["files"]:
        path = MANIFEST.parent.parent / entry["path"]
        assert path.exists(), entry["path"]
        assert path.stat().st_size == entry["bytes"]
        assert path.stat().st_size < 50 * 1024 * 1024
        import hashlib

        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        assert digest == entry["sha256"]


def test_iso_roundtrip():
    moment = datetime(2026, 10, 27, 12, 52, 52, tzinfo=UTC) + timedelta(microseconds=818000)
    assert iso(moment) == "2026-10-27T12:52:52.818000Z"
