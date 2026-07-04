from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Mapping, Protocol

from muscles.core import (
    ActionDispatcher as CoreActionDispatcher,
    ActionExecutionError,
    ActionNotFound,
    ActionPermissionDenied,
    ActionValidationError,
    StreamEvent,
    StreamResult,
    stream_events,
)


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

    def __init__(
        self,
        message: str = "",
        *,
        code: str | None = None,
        data: Any = None,
        status: int = 500,
        action_name: str | None = None,
    ):
        super().__init__(message)
        self.message = message
        self.code = code or self.__class__.code
        self.data = data
        self.status = status
        self.action_name = action_name


class SsePermissionDenied(SseStreamError):
    code = "permission_denied"


class SseValidationError(SseStreamError):
    code = "validation_error"


class _CoreDispatcher:
    def __init__(self, app):
        self._dispatcher = CoreActionDispatcher(app)

    def execute(self, action: str, payload: Mapping[str, Any] | None = None, **kwargs) -> Any:
        transport = kwargs.pop("transport", None)
        metadata = kwargs.pop("metadata", None)
        if kwargs:
            metadata = {**dict(metadata or {}), **kwargs}
        return self._dispatcher.execute(
            action,
            dict(payload or {}),
            transport=transport,
            metadata=metadata,
        )


class SseAdapter:
    """SSE transport projection over Muscles action execution."""

    allowed_events = {"progress", "log", "result", "error"}

    def __init__(self, dispatcher: ActionDispatcher, heartbeat_event: str | None = None):
        self.dispatcher = dispatcher
        self.heartbeat_event = heartbeat_event

    @classmethod
    def from_application(cls, app, heartbeat_event: str | None = None):
        return cls(_CoreDispatcher(app), heartbeat_event=heartbeat_event)

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

        stream = self._iter_result(result)
        if self.heartbeat_event:
            stream = self._with_heartbeat(stream, self.heartbeat_event)
        return SseResponse(stream=stream)

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
            for event in stream_events(self._to_core_stream(result)):
                yield self.format_event(self._from_core_event(event))
            return
        yield self.format_event(SseEvent(event="result", data=result))

    def _with_heartbeat(self, stream: Iterable[str], event_name: str) -> Iterator[str]:
        for chunk in stream:
            yield chunk
            yield self.format_event(SseEvent(event=event_name, data={"ok": True}))

    def _to_core_stream(self, result: Any) -> StreamResult:
        metadata: dict[str, Any] = {}
        close = None
        source = result
        if isinstance(result, StreamResult):
            metadata = dict(result.metadata)
            close = result.close_source
            source = result
        return StreamResult(source=self._adapt_stream_items(source), close=close, metadata=metadata)

    def _adapt_stream_items(self, source: Iterable[Any]) -> Iterator[StreamEvent | dict[str, Any]]:
        iterator = iter(source)
        try:
            for item in iterator:
                yield self._to_core_item(item)
        finally:
            close = getattr(iterator, "close", None)
            if callable(close):
                close()

    @staticmethod
    def _to_core_item(item: Any) -> StreamEvent | dict[str, Any]:
        if isinstance(item, StreamEvent):
            return item
        if isinstance(item, SseEvent):
            metadata: dict[str, Any] = {}
            if item.retry is not None:
                metadata["retry"] = item.retry
            return {"type": item.event, "data": item.data, "event_id": item.event_id, "metadata": metadata}
        if isinstance(item, Mapping):
            payload = dict(item)
            if "retry" in payload:
                metadata = dict(payload.get("metadata") or {})
                metadata.setdefault("retry", payload["retry"])
                payload["metadata"] = metadata
            return payload
        raise TypeError("SSE stream items must be SseEvent, core StreamEvent, or mapping")

    @staticmethod
    def _from_core_event(event: StreamEvent) -> SseEvent:
        retry = event.metadata.get("retry")
        return SseEvent(
            event=event.type,
            data=event.data,
            event_id=event.event_id,
            retry=retry if isinstance(retry, int) else None,
        )

    @staticmethod
    def _map_error(exc: Exception) -> SseStreamError:
        if isinstance(exc, ActionNotFound):
            return SseStreamError(
                exc.message,
                code=exc.code,
                data=exc.data,
                status=exc.status,
                action_name=exc.action_name,
            )
        if isinstance(exc, ActionValidationError):
            return SseValidationError(
                exc.message,
                code=exc.code,
                data=exc.data,
                status=exc.status,
                action_name=exc.action_name,
            )
        if isinstance(exc, ActionPermissionDenied):
            return SsePermissionDenied(
                exc.message,
                code=exc.code,
                data=exc.data,
                status=exc.status,
                action_name=exc.action_name,
            )
        if isinstance(exc, ActionExecutionError):
            return SseStreamError(
                exc.message,
                code=exc.code,
                data=exc.data,
                status=exc.status,
                action_name=exc.action_name,
            )
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
