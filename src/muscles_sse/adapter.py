from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Iterator


@dataclass(frozen=True)
class SseEvent:
    event: str
    data: dict[str, Any]
    event_id: str | None = None


class SseAdapter:
    """Converts Muscles typed events into SSE wire chunks."""

    allowed_events = {"progress", "log", "result", "error"}

    def stream(self, events: Iterable[SseEvent]) -> Iterator[str]:
        for event in events:
            if event.event not in self.allowed_events:
                raise ValueError(f"Unknown SSE event type: {event.event}")
            yield self.format_event(event)

    @staticmethod
    def format_event(event: SseEvent) -> str:
        lines = []
        if event.event_id is not None:
            lines.append(f"id: {event.event_id}")
        lines.append(f"event: {event.event}")
        lines.append(f"data: {json.dumps(event.data, ensure_ascii=False)}")
        lines.append("")
        return "\n".join(lines)
