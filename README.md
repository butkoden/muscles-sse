# Muscles SSE

Server-Sent Events adapter for Muscles streaming use cases.

This package should provide progress, logs, notifications, and AI output streams
over SSE while keeping the underlying workflow in the Muscles application model.

## Related Repositories

- [`muscles`](https://github.com/butkoden/muscles) - core `StreamResult`, `StreamEvent`, actions and dispatcher.
- [`muscles-asgi`](https://github.com/butkoden/muscles-asgi) - HTTP runtime that can expose SSE responses.
- [`muscles-ai`](https://github.com/butkoden/muscles-ai) - AI/RAG actions that may produce streaming output.
- [`muscles-jsonrpc`](https://github.com/butkoden/muscles-jsonrpc) - sibling protocol projection over actions.
- [`muscles-benchmarks`](https://github.com/butkoden/muscles-benchmarks) - streaming regression checks.

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
- application facade: `SseAdapter.from_application(app)` backed by core `ActionDispatcher`;
- stream normalization delegates to core `stream_events()` / `StreamResult`;
- typed events: `progress`, `log`, `result`, `error`;
- wire formatting: `id`, `event`, `retry`, `data`;
- `SseResponse` with SSE headers and status;
- heartbeat policy (optional);
- safe source close on stream completion/disconnect;
- structured core action error mapping: not found, validation, permission, execution.

This keeps SSE as a thin delivery layer. Business logic stays in Muscles actions.

### Run tests

```bash
python -m pytest -q
```
