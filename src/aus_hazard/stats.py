"""Small, explicit seismicity statistics.

Gutenberg-Richter b-value uses the Aki (1965) maximum-likelihood estimator
with the Utsu continuity correction:

    b = log10(e) / (mean(M) - (Mc - dM/2))

Mc is the magnitude bin with the most events (a maximum-curvature
approximation) plus 0.2, the usual correction from Woessner and Wiemer so the
estimate is not biased low. This is a description of the sample, not a
hazard forecast. Bins are 0.1 magnitude units. Estimates with fewer than 50
events at or above Mc are marked unstable.

ST-DBSCAN follows Birant and Kut: two events are neighbours when they are
within eps_km and eps_hours, and a cluster needs min_samples neighbours.
Depth is not a clustering dimension — Australian depth uncertainties are
often larger than the signal.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass

from aus_hazard.geo import haversine_km


def maximum_curvature_mc(magnitudes: list[float], bin_width: float = 0.1, correction: float = 0.2) -> float | None:
    if not magnitudes:
        return None
    counts: dict[float, int] = defaultdict(int)
    for mag in magnitudes:
        bucket = math.floor(mag / bin_width) * bin_width
        counts[round(bucket, 10)] += 1
    mode = max(counts.items(), key=lambda item: (item[1], item[0]))[0]
    return round(mode + correction, 4)


def gutenberg_richter_b(
    magnitudes: list[float],
    mc: float,
    bin_width: float = 0.1,
) -> tuple[float | None, int]:
    sample = [mag for mag in magnitudes if mag >= mc]
    if len(sample) < 2:
        return None, len(sample)
    mean_mag = sum(sample) / len(sample)
    denominator = mean_mag - (mc - bin_width / 2)
    if denominator <= 0:
        return None, len(sample)
    b_value = math.log10(math.e) / denominator
    return b_value, len(sample)


def b_value_stderr(b_value: float | None, n: int) -> float | None:
    """Shi and Bolt (1982) approximation: sigma_b ≈ 2.30 b^2 sigma_M / sqrt(n),
    with sigma_M ≈ 1 / (ln(10) b) for the exponential distribution, which
    reduces to ≈ b / sqrt(n)."""
    if b_value is None or n < 2:
        return None
    return b_value / math.sqrt(n)


@dataclass
class ClusterResult:
    labels: dict[str, int]
    n_clusters: int
    n_noise: int


def st_dbscan(
    points: list[tuple[str, float, float, float]],
    eps_km: float,
    eps_hours: float,
    min_samples: int,
) -> ClusterResult:
    """points are (id, lat, lon, time_hours). Labels start at 0; noise is -1."""
    n = len(points)
    if n == 0:
        return ClusterResult({}, 0, 0)

    # Grid in degrees. Latitude: 1 deg ≈ 111 km. Longitude scaled at the
    # mean latitude so a cell is about eps_km on a side.
    mean_lat = sum(point[1] for point in points) / n
    lat_cell = max(eps_km / 111.0, 1e-4)
    lon_scale = max(math.cos(math.radians(mean_lat)), 0.2)
    lon_cell = max(eps_km / (111.0 * lon_scale), 1e-4)
    grid: dict[tuple[int, int], list[int]] = defaultdict(list)
    for index, point in enumerate(points):
        key = (math.floor(point[1] / lat_cell), math.floor(point[2] / lon_cell))
        grid[key].append(index)

    def neighbours(index: int) -> list[int]:
        lat, lon, when = points[index][1], points[index][2], points[index][3]
        key = (math.floor(lat / lat_cell), math.floor(lon / lon_cell))
        found: list[int] = []
        for i in range(key[0] - 1, key[0] + 2):
            for j in range(key[1] - 1, key[1] + 2):
                for other in grid.get((i, j), []):
                    if other == index:
                        continue
                    olat, olon, owhen = points[other][1], points[other][2], points[other][3]
                    if abs(when - owhen) > eps_hours:
                        continue
                    if haversine_km(lat, lon, olat, olon) <= eps_km:
                        found.append(other)
        return found

    labels = [-99] * n
    cluster_id = 0
    for index in range(n):
        if labels[index] != -99:
            continue
        seed = neighbours(index)
        if len(seed) + 1 < min_samples:
            labels[index] = -1
            continue
        labels[index] = cluster_id
        queue = list(seed)
        while queue:
            current = queue.pop()
            if labels[current] == -1:
                labels[current] = cluster_id
            if labels[current] != -99:
                continue
            labels[current] = cluster_id
            current_neighbours = neighbours(current)
            if len(current_neighbours) + 1 >= min_samples:
                queue.extend(current_neighbours)
        cluster_id += 1

    mapping = {points[index][0]: labels[index] for index in range(n)}
    n_clusters = len({label for label in labels if label >= 0})
    n_noise = sum(1 for label in labels if label < 0)
    return ClusterResult(mapping, n_clusters, n_noise)
