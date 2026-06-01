# Muscles SSE

Server-Sent Events adapter for Muscles streaming use cases.

This package should provide progress, logs, notifications, and AI output streams
over SSE while keeping the underlying workflow in the Muscles application model.

## Concept Guardrails

- SSE streams must be backed by Muscles actions/events/jobs, not ad hoc handlers.
- The adapter must preserve shared rules/security and request context.
- Stream events must have typed names and payload schemas.
- SSE should be the simple default for one-way streaming before WebSocket is
  introduced.
- The package must be usable from ASGI without forcing unrelated runtime choices.

## Current Stage (Issue #1)

Implemented SSE transport projection over Muscles action execution:

- action dispatch path: `dispatcher.execute(action, payload, transport="sse")`;
- typed events: `progress`, `log`, `result`, `error`;
- wire formatting: `id`, `event`, `retry`, `data`;
- `SseResponse` with SSE headers and status;
- interval heartbeat policy (optional);
- safe source close on stream completion/disconnect;
- error mapping: permission/validation/internal.

This keeps SSE as a thin delivery layer. Business logic stays in Muscles actions.

## Interval heartbeat

English:

```python
adapter = SseAdapter(
    dispatcher,
    heartbeat_event="heartbeat",
    heartbeat_interval_seconds=15,
)
```

When the action stream is quiet longer than the configured interval, the SSE
transport emits a heartbeat event:

```text
event: heartbeat
data: {"ok": true}
```

User events keep their existing SSE format. Closing the response stream signals
the heartbeat worker to stop and closes the underlying source when it supports
`close()`.

Backpressure is preserved with a bounded one-item transport queue. For safe
disconnects, long-blocking sources should be cooperative: their `close()` method
must unblock the active `next()` call.

Русский:

```python
adapter = SseAdapter(
    dispatcher,
    heartbeat_event="heartbeat",
    heartbeat_interval_seconds=15,
)
```

Если action stream молчит дольше заданного интервала, SSE transport отправляет
heartbeat event:

```text
event: heartbeat
data: {"ok": true}
```

Пользовательские события сохраняют текущий SSE-формат. Закрытие response stream
останавливает heartbeat worker и закрывает исходный stream, если он поддерживает
`close()`.

Backpressure сохраняется через bounded transport queue на один элемент. Для
безопасного disconnect долгие blocking sources должны быть cooperative: их
`close()` должен разблокировать активный вызов `next()`.

### Run tests

```bash
python -m pytest -q
```
