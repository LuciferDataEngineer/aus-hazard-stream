"""Geodesy helpers and H3 indexing."""

from __future__ import annotations

import math
from typing import Any

import h3

EARTH_KM = 6371.0088


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * EARTH_KM * math.asin(min(1.0, math.sqrt(a)))


def h3_cells(lat: float, lon: float) -> dict[str, str]:
    return {
        "h3_res4": h3.latlng_to_cell(lat, lon, 4),
        "h3_res5": h3.latlng_to_cell(lat, lon, 5),
        "h3_res6": h3.latlng_to_cell(lat, lon, 6),
    }


def cell_polygon(cell: str) -> list[list[float]]:
    """GeoJSON ring (lon, lat), closed."""
    boundary = h3.cell_to_boundary(cell)
    ring = [[lon, lat] for lat, lon in boundary]
    if ring and ring[0] != ring[-1]:
        ring.append(ring[0])
    return ring


def in_box(lat: float, lon: float, box: dict[str, Any]) -> bool:
    return float(box["min_lat"]) <= lat <= float(box["max_lat"]) and float(box["min_lon"]) <= lon <= float(
        box["max_lon"]
    )
