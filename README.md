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

## Initial Goal

Expose typed progress/log/result events from a Muscles action as an SSE stream.

## Current Stage (Issue #1)

Implemented typed SSE adapter:

- event model: `progress`, `log`, `result`, `error`;
- strict unknown-event rejection;
- deterministic SSE wire formatting.

### Run tests

```bash
python -m pytest -q
```
