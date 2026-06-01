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
- heartbeat policy (optional);
- safe source close on stream completion/disconnect;
- error mapping: permission/validation/internal.

This keeps SSE as a thin delivery layer. Business logic stays in Muscles actions.

### Run tests

```bash
python -m pytest -q
```
