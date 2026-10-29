"""Data-quality checks.

Soda Core runs when the analytics database exists. The same thresholds are
also enforced in Python so CI still fails closed if the Soda CLI drifts.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from aus_hazard.config import ANALYTICS_DB, ROOT

SODA_CONFIG = ROOT / "quality" / "configuration.yml"
SODA_CHECKS = ROOT / "quality" / "checks.yml"


def python_checks(database: Path = ANALYTICS_DB) -> list[dict[str, object]]:
    con = duckdb.connect(str(database), read_only=True)
    checks: list[tuple[str, str]] = [
        ("fct_earthquake has rows", "select count(*) > 0 from fct_earthquake"),
        ("event ids unique", "select count(*) = count(distinct event_id) from fct_earthquake"),
        (
            "magnitude in range",
            "select min(magnitude) >= -1 and max(magnitude) < 10 from fct_earthquake",
        ),
        ("hotspots have rows", "select count(*) > 0 from fct_hotspot"),
        ("hotspot ids unique", "select count(*) = count(distinct hotspot_id) from fct_hotspot"),
        (
            "confidence in range",
            "select min(confidence) >= 0 and max(confidence) <= 100 from fct_hotspot",
        ),
        (
            "swarm mart has the news window",
            "select count(*) = 1 from mart_swarm_activity where scope = 'south_gippsland_swarm'",
        ),
        ("latency mart has three feeds", "select count(*) = 3 from mart_feed_latency_freshness"),
    ]
    results = []
    for name, sql in checks:
        row = con.execute(sql).fetchone()
        ok = bool(row and row[0])
        results.append({"check": name, "ok": ok})
    con.close()
    return results


def write_soda_config(database: Path = ANALYTICS_DB) -> None:
    SODA_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    SODA_CONFIG.write_text(f"data_source aus_hazard:\n  type: duckdb\n  path: {database}\n")


def main() -> None:
    results = python_checks()
    print(json.dumps(results, indent=2))
    failed = [row for row in results if not row["ok"]]
    if failed:
        raise SystemExit(f"{len(failed)} quality checks failed")
    try:
        from soda.scan import Scan

        write_soda_config()
        scan = Scan()
        scan.set_data_source_name("aus_hazard")
        scan.add_configuration_yaml_file(str(SODA_CONFIG))
        scan.add_sodacl_yaml_file(str(SODA_CHECKS))
        exit_code = scan.execute()
        print(scan.get_logs_text())
        if exit_code:
            raise SystemExit(exit_code)
    except SystemExit:
        raise
    except Exception as exc:  # Soda is best-effort if the API shifts
        print(f"Soda scan skipped: {exc}")


if __name__ == "__main__":
    main()
