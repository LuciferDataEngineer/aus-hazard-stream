"""Download free, keyless public feeds into a hashed Parquet snapshot.

Sources
-------
* USGS FDSN event query for the Australian bounding box, from 2006.
* Geoscience Australia earthquakes WFS (CC BY 4.0), Australian events from 2006.
* DEA Hotspots WFS ``public:hotspots_three_days`` (CC BY 4.0). The public
  service is a rolling ~72 hour window, so this snapshot cannot invent a
  longer fire season.
* Natural Earth admin-1 and populated places (public domain) and OurAirports
  (public domain) as small reference layers.

Each output file is kept under 50 MB. A manifest records the URL, fetch time,
row count, byte size, and sha256.
"""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from aus_hazard.config import (
    BBOX,
    CATALOG_END,
    HISTORICAL_START,
    MANIFEST,
    REFERENCE,
    SAMPLES,
    SNAPSHOTS,
    USER_AGENT,
)
from aus_hazard.normalize import hotspot_from_dea, quake_from_ga, quake_from_usgs

USGS_QUERY = "https://earthquake.usgs.gov/fdsnws/event/1/query"
GA_WFS = "https://earthquakes.ga.gov.au/geoserver/earthquakes/wfs"
DEA_WFS = "https://hotspots.dea.ga.gov.au/geoserver/public/ows"
NE_ADMIN = "https://naciscdn.org/naturalearth/50m/cultural/ne_50m_admin_1_states_provinces.zip"
# 10m places, not 50m: the 50m layer only has a few dozen Australian
# settlements, which is too coarse for a "near population" join.
NE_PLACES = "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_populated_places.zip"
AIRPORTS_CSV = "https://davidmegginson.github.io/ourairports-data/airports.csv"

GA_CQL = "located_in_australia='Y' AND origin_time >= '2006-01-01T00:00:00Z' AND origin_time < '2026-10-30T00:00:00Z'"


def _client() -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(180.0),
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        table = pa.table({"_empty": pa.array([], type=pa.string())})
        pq.write_table(table, path, compression="zstd")
        return
    # Columnar conversion keeps memory flatter than a list of huge dicts
    # passed through pandas. PyArrow infers types from the first rows, so
    # null-only columns are forced to string.
    columns: dict[str, list[Any]] = {key: [] for key in rows[0]}
    for row in rows:
        for key in columns:
            columns[key].append(row.get(key))
    arrays = []
    names = []
    for key, values in columns.items():
        names.append(key)
        if all(value is None for value in values):
            arrays.append(pa.array(values, type=pa.string()))
        else:
            arrays.append(pa.array(values))
    pq.write_table(pa.table(dict(zip(names, arrays, strict=True))), path, compression="zstd")


def fetch_usgs(
    client: httpx.Client,
    ingested_at: datetime,
    min_magnitude: float = 2.0,
    start: str = HISTORICAL_START[:10],
    end: str = CATALOG_END[:10],
) -> tuple[list[dict[str, Any]], str]:
    params: dict[str, str | int | float] = {
        "format": "geojson",
        "minlatitude": BBOX["min_latitude"],
        "maxlatitude": BBOX["max_latitude"],
        "minlongitude": BBOX["min_longitude"],
        "maxlongitude": BBOX["max_longitude"],
        "starttime": start,
        "endtime": end,
        "minmagnitude": min_magnitude,
        "orderby": "time",
    }
    response = client.get(USGS_QUERY, params=params)
    response.raise_for_status()
    payload = response.json()
    features = payload.get("features") or []
    rows = []
    for feature in features:
        row = quake_from_usgs(feature, ingested_at)
        if row is not None and row["event_type"] == "earthquake":
            rows.append(row)
    url = str(response.url)
    return rows, url


def fetch_ga(
    client: httpx.Client,
    ingested_at: datetime,
    page_size: int = 2000,
    cql: str = GA_CQL,
) -> tuple[list[dict[str, Any]], str]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    start = 0
    url = GA_WFS
    while True:
        response = client.get(
            GA_WFS,
            params={
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeNames": "earthquakes:earthquakes",
                "outputFormat": "application/json",
                "count": str(page_size),
                "startIndex": str(start),
                "sortBy": "earthquake_id",
                "CQL_FILTER": cql,
            },
        )
        response.raise_for_status()
        url = str(response.url)
        features = response.json().get("features") or []
        if not features:
            break
        for feature in features:
            row = quake_from_ga(feature, ingested_at)
            if row is None or row["event_id"] in seen:
                continue
            seen.add(row["event_id"])
            rows.append(row)
        if len(features) < page_size:
            break
        start += page_size
    return rows, url


def fetch_hotspots(
    client: httpx.Client, ingested_at: datetime, page_size: int = 4000
) -> tuple[list[dict[str, Any]], str]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    start = 0
    url = DEA_WFS
    while True:
        response = client.get(
            DEA_WFS,
            params={
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeNames": "public:hotspots_three_days",
                "outputFormat": "application/json",
                "count": str(page_size),
                "startIndex": str(start),
                "sortBy": "id",
            },
        )
        response.raise_for_status()
        url = str(response.url)
        features = response.json().get("features") or []
        if start == 0 and features:
            SAMPLES.mkdir(parents=True, exist_ok=True)
            (SAMPLES / "dea_hotspot_feature.json").write_text(json.dumps(features[0]))
        if not features:
            break
        for feature in features:
            row = hotspot_from_dea(feature, ingested_at)
            if row is None or row["hotspot_id"] in seen:
                continue
            seen.add(row["hotspot_id"])
            rows.append(row)
        print(f"  hotspots page start={start} features={len(features)} kept={len(rows)}", flush=True)
        if len(features) < page_size:
            break
        start += page_size
    return rows, url


def _unzip_shp(payload: bytes) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        for info in archive.infolist():
            if info.filename.lower().endswith((".shp", ".shx", ".dbf", ".prj", ".cpg")):
                files[Path(info.filename).name] = archive.read(info)
    return files


def _materialise_shp(files: dict[str, bytes], directory: Path, stem: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    shp = None
    for name, blob in files.items():
        target = directory / f"{stem}{Path(name).suffix.lower()}"
        target.write_bytes(blob)
        if target.suffix == ".shp":
            shp = target
    if shp is None:
        raise RuntimeError(f"no shapefile in {stem}")
    return shp


def fetch_reference(client: httpx.Client) -> list[dict[str, Any]]:
    """Natural Earth states and places, plus Australian OurAirports rows."""
    import duckdb

    REFERENCE.mkdir(parents=True, exist_ok=True)
    notes: list[dict[str, Any]] = []
    tmp = REFERENCE / "_tmp"
    tmp.mkdir(parents=True, exist_ok=True)

    admin = client.get(NE_ADMIN)
    admin.raise_for_status()
    admin_shp = _materialise_shp(_unzip_shp(admin.content), tmp / "admin", "admin")
    places = client.get(NE_PLACES)
    places.raise_for_status()
    places_shp = _materialise_shp(_unzip_shp(places.content), tmp / "places", "places")

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    admin_cols = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM st_read('{admin_shp}')").fetchall()]
    place_cols = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM st_read('{places_shp}')").fetchall()]
    (REFERENCE / "natural_earth_columns.json").write_text(
        json.dumps({"admin": admin_cols, "places": place_cols}, indent=2)
    )

    admin_name = "name" if "name" in admin_cols else "NAME"
    admin_iso = "iso_3166_2" if "iso_3166_2" in admin_cols else "ISO_3166_2"
    country_col = "adm0_a3" if "adm0_a3" in admin_cols else ("ADM0_A3" if "ADM0_A3" in admin_cols else "iso_a2")
    country_value = "AUS" if "a3" in country_col.lower() else "AU"
    states = con.execute(
        f"""
        SELECT {admin_name} AS state_name,
               {admin_iso} AS state_code,
               ST_AsGeoJSON(ST_Simplify(geom, 0.05)) AS geometry
        FROM st_read('{admin_shp}')
        WHERE {country_col} = '{country_value}'
        """
    ).fetchall()
    features = []
    for name, code, geom in states:
        features.append(
            {
                "type": "Feature",
                "properties": {"state_name": name, "state_code": code},
                "geometry": json.loads(geom),
            }
        )
    states_path = REFERENCE / "au_states.geojson"
    states_path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))
    notes.append(
        {
            "path": str(states_path.relative_to(MANIFEST.parent.parent)),
            "source_url": NE_ADMIN,
            "licence": "Public domain (Natural Earth)",
            "rows": len(features),
            "sha256": _sha256(states_path),
            "bytes": states_path.stat().st_size,
        }
    )

    pop_col = "pop_max" if "pop_max" in place_cols else "POP_MAX"
    pname = "name" if "name" in place_cols else "NAME"
    plat = "latitude" if "latitude" in place_cols else "LATITUDE"
    plon = "longitude" if "longitude" in place_cols else "LONGITUDE"
    pcountry = "adm0_a3" if "adm0_a3" in place_cols else ("ADM0_A3" if "ADM0_A3" in place_cols else "iso_a2")
    pvalue = "AUS" if "a3" in pcountry.lower() else "AU"
    place_tuples = con.execute(
        f"""
        SELECT {pname} AS name, {pop_col} AS pop_max, {plat} AS latitude, {plon} AS longitude
        FROM st_read('{places_shp}')
        WHERE {pcountry} = '{pvalue}' AND {pop_col} >= 5000
        """
    ).fetchall()
    place_rows = [
        {
            "name": name,
            "pop_max": None if pop is None else int(pop),
            "latitude": float(lat),
            "longitude": float(lon),
        }
        for name, pop, lat, lon in place_tuples
    ]
    places_path = REFERENCE / "populated_places.parquet"
    _write_parquet(places_path, place_rows)
    notes.append(
        {
            "path": str(places_path.relative_to(MANIFEST.parent.parent)),
            "source_url": NE_PLACES,
            "licence": "Public domain (Natural Earth)",
            "rows": len(place_rows),
            "sha256": _sha256(places_path),
            "bytes": places_path.stat().st_size,
        }
    )

    airports = client.get(AIRPORTS_CSV)
    airports.raise_for_status()
    import csv

    reader = csv.DictReader(io.StringIO(airports.text))
    kept = []
    for row in reader:
        if row.get("iso_country") != "AU":
            continue
        if row.get("type") not in {"large_airport", "medium_airport"}:
            continue
        kept.append(
            {
                "ident": row.get("ident"),
                "iata": row.get("iata_code") or None,
                "name": row.get("name"),
                "type": row.get("type"),
                "municipality": row.get("municipality") or None,
                "latitude": float(row["latitude_deg"]),
                "longitude": float(row["longitude_deg"]),
            }
        )
    airports_path = REFERENCE / "airports.parquet"
    _write_parquet(airports_path, kept)
    notes.append(
        {
            "path": str(airports_path.relative_to(MANIFEST.parent.parent)),
            "source_url": AIRPORTS_CSV,
            "licence": "Public domain (OurAirports)",
            "rows": len(kept),
            "sha256": _sha256(airports_path),
            "bytes": airports_path.stat().st_size,
        }
    )
    con.close()
    return notes


def _entry(path: Path, url: str, licence: str, rows: int, fetched_at: str, extra: dict[str, Any]) -> dict[str, Any]:
    relative = str(path.relative_to(MANIFEST.parent.parent))
    return {
        "path": relative,
        "source_url": url,
        "licence": licence,
        "rows": rows,
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
        "fetched_at": fetched_at,
        "free": True,
        "api_key": False,
        **extra,
    }


def build_snapshot(include_reference: bool = True) -> dict[str, Any]:
    ingested_at = datetime.now(UTC)
    fetched = ingested_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    SNAPSHOTS.mkdir(parents=True, exist_ok=True)
    SAMPLES.mkdir(parents=True, exist_ok=True)
    files: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    with _client() as client:
        try:
            print("fetching USGS…", flush=True)
            usgs_rows, usgs_url = fetch_usgs(client, ingested_at)
            usgs_path = SNAPSHOTS / "usgs_earthquakes.parquet"
            _write_parquet(usgs_path, usgs_rows)
            if usgs_rows:
                (SAMPLES / "usgs_contract_event.json").write_text(json.dumps(usgs_rows[0], indent=2))
            files.append(
                _entry(
                    usgs_path,
                    usgs_url,
                    "Public domain (U.S. Geological Survey)",
                    len(usgs_rows),
                    fetched,
                    {
                        "publisher": "U.S. Geological Survey",
                        "dataset": "FDSN Event Web Service, Australian bounding box, M>=2, 2006-01-01 to 2026-10-30",
                        "coverage": "lat -45 to -9, lon 110 to 160",
                    },
                )
            )
        except Exception as exc:  # network failures are recorded, not fatal
            errors.append({"source": "usgs", "error": str(exc)})
            print("USGS failed:", exc, flush=True)

        try:
            print("fetching Geoscience Australia…", flush=True)
            ga_rows, ga_url = fetch_ga(client, ingested_at)
            ga_path = SNAPSHOTS / "ga_earthquakes.parquet"
            _write_parquet(ga_path, ga_rows)
            if ga_rows:
                (SAMPLES / "ga_contract_event.json").write_text(json.dumps(ga_rows[0], indent=2))
            files.append(
                _entry(
                    ga_path,
                    ga_url,
                    "CC BY 4.0 (Commonwealth of Australia / Geoscience Australia)",
                    len(ga_rows),
                    fetched,
                    {
                        "publisher": "Geoscience Australia",
                        "dataset": "earthquakes:earthquakes WFS, located_in_australia=Y, 2006-01-01 to 2026-10-30",
                        "coverage": "events GA flags as located in Australia",
                    },
                )
            )
        except Exception as exc:
            errors.append({"source": "ga", "error": str(exc)})
            print("GA failed:", exc, flush=True)

        try:
            print("fetching DEA hotspots…", flush=True)
            hot_rows, hot_url = fetch_hotspots(client, ingested_at)
            hot_path = SNAPSHOTS / "dea_hotspots.parquet"
            _write_parquet(hot_path, hot_rows)
            files.append(
                _entry(
                    hot_path,
                    hot_url,
                    "CC BY 4.0 (Commonwealth of Australia / Geoscience Australia). BRIGHT/Himawari algorithm attribution: Engel, Jones and Reinke.",
                    len(hot_rows),
                    fetched,
                    {
                        "publisher": "Digital Earth Australia / Geoscience Australia",
                        "dataset": "public:hotspots_three_days WFS (rolling ~72 hour public window)",
                        "coverage": "Australia, all satellites on the public layer at fetch time",
                    },
                )
            )
        except Exception as exc:
            errors.append({"source": "dea", "error": str(exc)})
            print("DEA failed:", exc, flush=True)

        if include_reference:
            try:
                print("fetching reference layers…", flush=True)
                for note in fetch_reference(client):
                    note["fetched_at"] = fetched
                    note["free"] = True
                    note["api_key"] = False
                    files.append(note)
            except Exception as exc:
                errors.append({"source": "reference", "error": str(exc)})
                print("reference failed:", exc, flush=True)

    manifest = {
        "project": "aus-hazard-stream",
        "created_at": fetched,
        "note": "Snapshot of keyless public feeds. Rebuild with `make backfill`. CI replays these files and does not call the network.",
        "files": files,
        "errors": errors,
    }
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(manifest, indent=2))
    print(
        json.dumps(
            {
                "files": [{k: f[k] for k in ("path", "rows", "bytes")} for f in files],
                "errors": errors,
            },
            indent=2,
        )
    )
    return manifest


def main() -> None:
    build_snapshot()


if __name__ == "__main__":
    main()
