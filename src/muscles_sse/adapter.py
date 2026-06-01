from __future__ import annotations

import json
import queue
import threading
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Mapping, Protocol


@dataclass(frozen=True)
class SseEvent:
    event: str
    data: Any
    event_id: str | None = None
    retry: int | None = None


@dataclass(frozen=True)
class SseResponse:
    stream: Iterable[str]
    status: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    content_type: str = "text/event-stream; charset=utf-8"

    def __post_init__(self) -> None:
        merged = {
            "Content-Type": self.content_type,
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
        merged.update(self.headers)
        object.__setattr__(self, "headers", merged)


class ActionDispatcher(Protocol):
    def execute(self, action: str, payload: Mapping[str, Any] | None = None, **kwargs) -> Any:
        ...


class SseStreamError(Exception):
    code = "internal_error"


class SsePermissionDenied(SseStreamError):
    code = "permission_denied"


class SseValidationError(SseStreamError):
    code = "validation_error"


class SseAdapter:
    """SSE transport projection over Muscles action execution."""

    allowed_events = {"progress", "log", "result", "error"}
    default_heartbeat_interval_seconds = 15.0
    worker_join_timeout_seconds = 0.1

    def __init__(
        self,
        dispatcher: ActionDispatcher,
        heartbeat_event: str | None = None,
        heartbeat_interval_seconds: float | None = None,
    ):
        self.dispatcher = dispatcher
        self.heartbeat_event = heartbeat_event
        if heartbeat_interval_seconds is not None and heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be greater than 0")
        self.heartbeat_interval_seconds = heartbeat_interval_seconds

    def stream_action(
        self,
        action: str,
        payload: Mapping[str, Any] | None = None,
        *,
        context: Mapping[str, Any] | None = None,
    ) -> SseResponse:
        context = dict(context or {})
        try:
            result = self.dispatcher.execute(action, payload, transport="sse", **context)
        except Exception as exc:
            mapped = self._map_error(exc)
            raise mapped from exc

        return SseResponse(stream=self._iter_result(result))

    @staticmethod
    def _unwrap_result(result: Any) -> tuple[Any, bool | None]:
        # muscles.core.ActionDispatcher returns ActionResult with value/is_stream.
        if hasattr(result, "value") and hasattr(result, "is_stream"):
            return result.value, bool(result.is_stream)
        return result, None

    def _iter_result(self, result: Any) -> Iterator[str]:
        result, is_stream = self._unwrap_result(result)
        should_stream = (
            is_stream
            if is_stream is not None
            else isinstance(result, Iterable) and not isinstance(result, (str, bytes, bytearray, dict))
        )
        if should_stream:
            source = iter(result)
            if self.heartbeat_event:
                interval = self.heartbeat_interval_seconds or self.default_heartbeat_interval_seconds
                yield from self._stream_source_with_heartbeat(source, self.heartbeat_event, interval)
                return
            try:
                for item in source:
                    try:
                        yield self.format_event(self._coerce_event(item))
                    except Exception as exc:
                        yield self.format_event(
                            SseEvent(event="error", data={"code": self._map_error(exc).code, "message": str(exc)})
                        )
                        break
            except Exception as exc:
                yield self.format_event(SseEvent(event="error", data={"code": self._map_error(exc).code, "message": str(exc)}))
            finally:
                close = getattr(source, "close", None)
                if callable(close):
                    close()
            return
        yield self.format_event(SseEvent(event="result", data=result))

    def _stream_source_with_heartbeat(
        self,
        source: Iterator[Any],
        event_name: str,
        interval_seconds: float,
    ) -> Iterator[str]:
        chunks: queue.Queue[tuple[str, str | None]] = queue.Queue()
        stop = threading.Event()

        def read_source() -> None:
            try:
                while not stop.is_set():
                    try:
                        item = next(source)
                        chunk = self.format_event(self._coerce_event(item))
                    except StopIteration:
                        chunks.put(("done", None))
                        break
                    except Exception as exc:
                        chunks.put(
                            (
                                "chunk",
                                self.format_event(
                                    SseEvent(
                                        event="error",
                                        data={"code": self._map_error(exc).code, "message": str(exc)},
                                    )
                                ),
                            )
                        )
                        chunks.put(("done", None))
                        break
                    if stop.is_set():
                        break
                    chunks.put(("chunk", chunk))
            finally:
                self._close_source(source)

        worker = threading.Thread(target=read_source, daemon=True)
        worker.start()
        try:
            while True:
                try:
                    kind, chunk = chunks.get(timeout=interval_seconds)
                except queue.Empty:
                    yield self.format_event(SseEvent(event=event_name, data={"ok": True}))
                    continue
                if kind == "done":
                    break
                if chunk is not None:
                    yield chunk
        finally:
            stop.set()
            self._close_source(source)
            worker.join(timeout=min(interval_seconds, self.worker_join_timeout_seconds))

    @staticmethod
    def _close_source(source: Iterator[Any]) -> None:
        close = getattr(source, "close", None)
        if callable(close):
            try:
                close()
            except (RuntimeError, ValueError):
                pass

    def _coerce_event(self, item: Any) -> SseEvent:
        if isinstance(item, SseEvent):
            if item.event not in self.allowed_events and item.event != (self.heartbeat_event or ""):
                raise ValueError(f"Unknown SSE event type: {item.event}")
            return item
        if isinstance(item, Mapping):
            event = str(item.get("event", "progress"))
            data = item.get("data")
            event_id = item.get("id")
            retry = item.get("retry")
            coerced = SseEvent(event=event, data=data, event_id=event_id, retry=retry)
            if coerced.event not in self.allowed_events and coerced.event != (self.heartbeat_event or ""):
                raise ValueError(f"Unknown SSE event type: {coerced.event}")
            return coerced
        raise TypeError("SSE stream items must be SseEvent or mapping")

    @staticmethod
    def _map_error(exc: Exception) -> SseStreamError:
        name = exc.__class__.__name__.lower()
        if "permission" in name or "forbidden" in name:
            return SsePermissionDenied(str(exc))
        if "validation" in name:
            return SseValidationError(str(exc))
        return SseStreamError(str(exc))

    @staticmethod
    def format_event(event: SseEvent) -> str:
        lines = []
        if event.event_id is not None:
            lines.append(f"id: {event.event_id}")
        if event.retry is not None:
            lines.append(f"retry: {event.retry}")
        lines.append(f"event: {event.event}")
        lines.append(f"data: {json.dumps(event.data, ensure_ascii=False, default=str)}")
        lines.append("")
        return "\n".join(lines)
