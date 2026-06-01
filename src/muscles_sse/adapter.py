from __future__ import annotations

import json
import queue
import threading
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Mapping, Protocol

from muscles.core import StreamEvent, stream_events


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
    stream_queue_size = 1
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
            source = iter(stream_events(result))
            if self.heartbeat_event:
                interval = self.heartbeat_interval_seconds or self.default_heartbeat_interval_seconds
                yield from self._stream_source_with_heartbeat(source, self.heartbeat_event, interval, close_target=result)
                return
            try:
                for item in source:
                    yield self.format_event(item)
            except Exception as exc:
                yield self.format_event(SseEvent(event="error", data={"code": self._map_error(exc).code, "message": str(exc)}))
            finally:
                self._close_source(source)
            return
        yield self.format_event(SseEvent(event="result", data=result))

    def _stream_source_with_heartbeat(
        self,
        source: Iterator[Any],
        event_name: str,
        interval_seconds: float,
        close_target: Any | None = None,
    ) -> Iterator[str]:
        chunks: queue.Queue[tuple[str, str | None]] = queue.Queue(maxsize=self.stream_queue_size)
        stop = threading.Event()

        def put_chunk(kind: str, chunk: str | None) -> bool:
            while not stop.is_set():
                try:
                    chunks.put((kind, chunk), timeout=self.worker_join_timeout_seconds)
                    return True
                except queue.Full:
                    continue
            return False

        def read_source() -> None:
            try:
                while not stop.is_set():
                    try:
                        item = next(source)
                        chunk = self.format_event(item)
                    except StopIteration:
                        put_chunk("done", None)
                        break
                    except Exception as exc:
                        put_chunk(
                            "chunk",
                            self.format_event(
                                SseEvent(
                                    event="error",
                                    data={"code": self._map_error(exc).code, "message": str(exc)},
                                )
                            ),
                        )
                        put_chunk("done", None)
                        break
                    if stop.is_set():
                        break
                    if not put_chunk("chunk", chunk):
                        break
            finally:
                self._close_source(source)
                if close_target is not None:
                    self._close_source(close_target)

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
            if close_target is not None:
                self._close_source(close_target)
            worker.join(timeout=min(interval_seconds, self.worker_join_timeout_seconds))

    @staticmethod
    def _close_source(source: Iterator[Any]) -> None:
        close = getattr(source, "close", None)
        if close is None:
            close = getattr(source, "close_source", None)
        if callable(close):
            try:
                close()
            except (RuntimeError, ValueError):
                pass

    @staticmethod
    def _map_error(exc: Exception) -> SseStreamError:
        name = exc.__class__.__name__.lower()
        if "permission" in name or "forbidden" in name:
            return SsePermissionDenied(str(exc))
        if "validation" in name:
            return SseValidationError(str(exc))
        return SseStreamError(str(exc))

    @staticmethod
    def format_event(event: SseEvent | StreamEvent) -> str:
        lines = []
        event_id = getattr(event, "event_id", None)
        metadata = getattr(event, "metadata", {}) or {}
        retry = getattr(event, "retry", None) or metadata.get("retry")
        event_name = getattr(event, "event", None) or getattr(event, "type")
        if event_id is not None:
            lines.append(f"id: {event_id}")
        if retry is not None:
            lines.append(f"retry: {retry}")
        lines.append(f"event: {event_name}")
        lines.append(f"data: {json.dumps(event.data, ensure_ascii=False, default=str)}")
        lines.append("")
        return "\n".join(lines)
