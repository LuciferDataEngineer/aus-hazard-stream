"""Iceberg schema evolution and snapshot time travel on a tiny local catalog."""

from pathlib import Path

from aus_hazard.lake import demo_time_travel


def test_time_travel_and_schema_evolution(tmp_path: Path):
    result = demo_time_travel(tmp_path / "warehouse", tmp_path / "catalog.db")
    assert result["previous_magnitude"] == [3.5]
    assert result["current_magnitude"] == [3.6]
    assert "region_name" in result["fields"]
