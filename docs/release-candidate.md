# SSE RC checklist

The SSE RC must demonstrate an end-to-end `StreamResult` through the adapter:

```bash
PYTHONPATH=../muscles/src:src python -m pytest -q
python -m build --wheel --sdist
```

The test suite covers event framing, progress/log/result/error events,
heartbeat, source closing and core action error mapping. SSE owns delivery only;
business execution remains in `ActionDispatcher`.
