# Invalid Test Data

This directory contains intentionally malformed OTLP data files used for negative testing.

## Test Files

### `log_invalid_severity.json`
**Tests:** Invalid enum value handling for severity numbers
**Invalid Field:** `severityNumber: "INVALID_SEVERITY_VALUE"`
**Expected:** Parser should reject and return error

### `trace_invalid_base64.json`
**Tests:** Invalid base64 encoding in trace IDs
**Invalid Field:** `traceId: "!!!INVALID_BASE64!!!"`, `spanId: "INVALID=="`
**Expected:** Base64 decoder should fail and propagate error

### `trace_invalid_kind.json`
**Tests:** Invalid enum value for span kind
**Invalid Field:** `kind: "SPAN_KIND_INVALID_TYPE"`
**Expected:** Parser should reject invalid span kind

### `trace_mixed_encoding.json`
**Tests:** Invalid encoding (neither hex nor base64)
**Invalid Field:** `traceId: "zzz"`, `spanId: "xyz"`
**Expected:** Both hex and base64 decoders should fail

### `metrics_invalid_temporality.json`
**Tests:** Invalid aggregation temporality enum
**Invalid Field:** `aggregationTemporality: "INVALID_TEMPORALITY_VALUE"`
**Expected:** Parser should reject invalid temporality

### `malformed.json`
**Tests:** Malformed JSON syntax
**Invalid:** Missing closing braces, missing comma
**Expected:** JSON parser should fail with syntax error

## Purpose

These tests ensure that:
1. Invalid data is properly rejected (no silent failures)
2. Error messages are clear and actionable
3. The parser fails fast on bad input
4. Enum validation works correctly
5. Encoding validation (hex/base64) is robust

## Adding New Invalid Tests

When adding new invalid test files:
1. Create the file in this directory
2. Add a corresponding test in `tests/e2e.rs`
3. Document the file in this README
4. Ensure the test verifies `result.is_err()`
