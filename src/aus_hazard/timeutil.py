"""UTC timestamp helpers. All stored times are timezone-aware UTC."""

from __future__ import annotations

from datetime import UTC, datetime


def parse_time(value: str | datetime | None) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    # Trim fractional seconds longer than 6 digits (Python limit).
    if "." in text:
        head, tail = text.split(".", 1)
        plus = ""
        frac = tail
        for sep in ("+", "-"):
            # timezone offset after the fraction; dates don't have a bare minus here
            idx = tail.find(sep)
            if idx > 0:
                frac = tail[:idx]
                plus = tail[idx:]
                break
        text = f"{head}.{frac[:6]}{plus}"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def to_millis(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def from_millis(millis: int) -> datetime:
    return datetime.fromtimestamp(millis / 1000, tz=UTC)
