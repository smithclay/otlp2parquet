# Test Data

Deterministic OpenTelemetry test data for logs, metrics, and traces in JSON, JSONL, and Protobuf formats.

## Generation

All files are generated using:

```bash
./scripts/generate_testdata.py
```

This script uses a fixed seed (42) to ensure reproducible test data across runs.

## Files

- **Logs:** `log.json`, `logs.jsonl`, `logs.pb`
- **Metrics:** 6 types × 3 formats (gauge, sum, histogram, exponential_histogram, summary, mixed)
- **Traces:** `trace.json`, `trace.pb`, `traces.jsonl`, `traces.pb`

Run `./scripts/generate_testdata.py --help` for more options.
