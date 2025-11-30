# Live Browser Observability Demo Design

## Overview

A single-page application that captures real browser telemetry (logs, metrics, traces), converts it to Arrow via WASM-compiled `otlp2parquet-core`, stores it in DuckDB-WASM, and visualizes it with Perspective - all running entirely in the browser.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Browser                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │   OTel JS    │    │ otlp2parquet │    │    DuckDB-WASM       │  │
│  │     SDK      │───▶│    (WASM)    │───▶│  (Arrow Tables)      │  │
│  │              │    │              │    │                      │  │
│  │ • Traces     │    │ OTLP JSON    │    │ • logs               │  │
│  │ • Metrics    │    │   → Arrow    │    │ • metrics_gauge      │  │
│  │ • Logs       │    │              │    │ • traces             │  │
│  └──────────────┘    └──────────────┘    └──────────┬───────────┘  │
│                                                      │              │
│         ┌────────────────────────────────────────────┘              │
│         ▼                                                           │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    UI Layout                                 │   │
│  │  ┌─────────────────┐  ┌───────────────────────────────────┐ │   │
│  │  │   SQL Panel     │  │         Dashboard                  │ │   │
│  │  │   (30%)         │  │         (70%)                      │ │   │
│  │  │                 │  │  ┌─────────┐ ┌─────────┐          │ │   │
│  │  │  [SQL Editor]   │  │  │  Logs   │ │ Metrics │          │ │   │
│  │  │                 │  │  │ (table) │ │ (chart) │          │ │   │
│  │  │  [Perspective]  │  │  └─────────┘ └─────────┘          │ │   │
│  │  │   (results)     │  │  ┌───────────────────────────────┐ │ │   │
│  │  │                 │  │  │     Traces (datagrid)         │ │ │   │
│  │  └─────────────────┘  │  └───────────────────────────────┘ │ │   │
│  │                       │  ┌───────────────────────────────┐ │ │   │
│  │                       │  │   Interactive Playground      │ │ │   │
│  │                       │  │ [Slow API] [Error] [Nested]   │ │ │   │
│  │                       │  └───────────────────────────────┘ │ │   │
│  │                       └───────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## Telemetry Collection

Uses the official OpenTelemetry JavaScript SDK with a custom local exporter.

### Traces
- `WebTracerProvider` with auto-instrumentation for fetch, document load, user interaction
- Custom spans from playground actions

### Metrics
- `MeterProvider` capturing Web Vitals (LCP, FID, CLS, TTFB, INP)
- Resource timing, memory usage, DOM node count

### Logs
- `LoggerProvider` intercepting console.log/warn/error
- Uncaught exceptions via `window.onerror`
- Promise rejections via `unhandledrejection`

### Custom Local Exporter

```javascript
class LocalArrowExporter {
  export(items, resultCallback) {
    const otlpJson = serializeToOtlpJson(items);
    const arrowBytes = wasm.signal_to_arrow(otlpJson);
    ingestToDuckDB(arrowBytes);
    resultCallback({ code: 0 });
  }
}
```

## WASM Integration

Build `otlp2parquet-core` to WASM with a new make target:

```makefile
wasm-demo:
	wasm-pack build crates/otlp2parquet-core --target web \
		--out-dir ../../docs/query-demo/wasm \
		--features wasm
```

Add `#[wasm_bindgen]` exports in `otlp2parquet-core/src/lib.rs` behind a `wasm` feature flag:

```rust
#[cfg(feature = "wasm")]
#[wasm_bindgen]
pub fn logs_to_arrow(logs_json: &str) -> Result<Uint8Array, JsError>

#[cfg(feature = "wasm")]
#[wasm_bindgen]
pub fn metrics_to_arrow(metrics_json: &str) -> Result<Uint8Array, JsError>

#[cfg(feature = "wasm")]
#[wasm_bindgen]
pub fn traces_to_arrow(traces_json: &str) -> Result<Uint8Array, JsError>
```

## Perspective Dashboard

### Widget Configuration

| Widget | View Type | Key Columns | Updates |
|--------|-----------|-------------|---------|
| Logs | `datagrid` | Timestamp, SeverityText, Body | Append, scroll to latest |
| Metrics | `y_line` | Timestamp (x), Value (y), MetricName (color) | Rolling window |
| Traces | `datagrid` | SpanName, Duration, Status | Append, sort by time |
| SQL Results | `datagrid` | Dynamic from query | On query run |

### Streaming Updates

Each Perspective viewer shares a DuckDB connection. On telemetry flush:
1. Insert new Arrow data to DuckDB tables
2. Each widget re-runs its backing query
3. Perspective's `table.update()` appends new rows

## Interactive Playground

| Button | Action | Telemetry Generated |
|--------|--------|---------------------|
| Slow API | `setTimeout` mock fetch | Trace span with 2-3s duration |
| Throw Error | Throws uncaught exception | Log with ERROR severity + stack trace |
| Log Message | `console.log()` | Log with INFO severity |
| Nested Calls | 3 chained async operations | Parent span + 3 child spans |
| Web Vital | Triggers layout shift | CLS metric data point |

## Initialization Flow

```
1. Load page → Show "Initializing..." status

2. Initialize in parallel:
   ├── DuckDB-WASM load + instantiate
   ├── otlp2parquet WASM load
   └── Perspective engine load

3. Create DuckDB tables (otlp2parquet schemas):
   ├── CREATE TABLE logs (...)
   ├── CREATE TABLE metrics_gauge (...)
   └── CREATE TABLE traces (...)

4. Initialize OTel SDK:
   ├── TracerProvider + LocalArrowExporter
   ├── MeterProvider + LocalArrowExporter
   └── LoggerProvider + LocalArrowExporter

5. Create Perspective viewers (empty)

6. Start telemetry collection
   └── Page load span + initial metrics captured

7. First flush (after 2s or 50 events):
   ├── OTel export → OTLP JSON
   ├── WASM convert → Arrow IPC
   ├── DuckDB insert
   └── Perspective update

8. Status: "Ready" - streaming mode (1s flush interval)
```

## File Structure

```
docs/query-demo/
├── index.html           # Main page with split-panel layout
├── style.css            # Dark theme, Perspective styling
├── app.js               # Main orchestrator
├── telemetry.js         # OTel SDK setup, LocalArrowExporter
├── playground.js        # Interactive button handlers
├── wasm/                # Built by `make wasm-demo`, gitignored
│   ├── otlp2parquet_core.js
│   └── otlp2parquet_core_bg.wasm
└── data/                # Existing sample parquet files (fallback)
```

## Dependencies (CDN ESM)

- `@duckdb/duckdb-wasm@1.31.0`
- `@finos/perspective@3.1.0`
- `@finos/perspective-viewer@3.1.0`
- `@finos/perspective-viewer-datagrid@3.1.0`
- `@finos/perspective-viewer-d3fc@3.1.0`
- `@opentelemetry/sdk-trace-web@1.25.0`
- `@opentelemetry/exporter-trace-otlp-http@0.52.0`
- `@opentelemetry/sdk-metrics@1.25.0`
- `@opentelemetry/sdk-logs@0.52.0`

## Changes Required

### New Files
- `docs/query-demo/telemetry.js`
- `docs/query-demo/playground.js`

### Modified Files
- `docs/query-demo/index.html` - new split-panel layout
- `docs/query-demo/style.css` - dashboard + Perspective styles
- `docs/query-demo/app.js` - orchestrator rewrite
- `Makefile` - add `wasm-demo` target
- `crates/otlp2parquet-core/Cargo.toml` - add `wasm` feature
- `crates/otlp2parquet-core/src/lib.rs` - add `#[wasm_bindgen]` exports

## Constraints

- No server required - everything client-side
- Uses actual `otlp2parquet-core` schemas (ClickHouse-compatible PascalCase)
- Target WASM size: stay within 3MB compressed limit
- Hybrid flow: initial batch populates dashboard, then streaming updates
