"""Event-time watermark. Late events are flagged and still applied."""

from __future__ import annotations

from dataclasses import dataclass, field


def hour_start_ms(event_ms: int) -> int:
    hour = 3_600_000
    return event_ms - (event_ms % hour)


@dataclass
class Watermark:
    lateness_ms: int
    max_event_ms: int | None = None
    closed_windows: set[int] = field(default_factory=set)
    late_events: int = 0
    on_time_events: int = 0

    def current_ms(self) -> int | None:
        if self.max_event_ms is None:
            return None
        return self.max_event_ms - self.lateness_ms

    def observe(self, event_ms: int) -> str:
        """Classify an event against the watermark *before* advancing it.

        A tumbling hour is closed once the watermark passes the end of the
        hour. Events for a closed hour are late revisions: counted, not dropped.
        """
        window_start = hour_start_ms(event_ms)
        window_end = window_start + 3_600_000
        watermark = self.current_ms()
        late = watermark is not None and window_end <= watermark
        if late:
            self.closed_windows.add(window_start)
            self.late_events += 1
        else:
            self.on_time_events += 1
        if self.max_event_ms is None or event_ms > self.max_event_ms:
            self.max_event_ms = event_ms
        advanced = self.current_ms()
        if advanced is not None:
            closed_start = hour_start_ms(advanced) - 3_600_000
            if closed_start + 3_600_000 <= advanced:
                self.closed_windows.add(closed_start)
        return "late" if late else "on_time"
