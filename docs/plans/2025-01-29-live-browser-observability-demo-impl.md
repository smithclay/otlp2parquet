# Live Browser Observability Demo Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a browser-based demo that captures live telemetry via OTel JS SDK, converts it to Arrow using WASM-compiled otlp2parquet-core, stores in DuckDB-WASM, and visualizes with Perspective.

**Architecture:** OTel JS SDK exports OTLP JSON → custom LocalArrowExporter calls otlp2parquet-core WASM → Arrow IPC bytes → DuckDB-WASM tables → Perspective viewers. Split-panel UI with SQL editor + dashboard.

**Tech Stack:** OTel JS SDK, wasm-bindgen, wasm-pack, DuckDB-WASM, Perspective, vanilla JS (ES modules via CDN)

---

## Task 1: Add WASM Feature Flag to otlp2parquet-core

**Files:**
- Modify: `crates/otlp2parquet-core/Cargo.toml`

**Step 1: Add wasm-bindgen dependency behind feature flag**

Add to `crates/otlp2parquet-core/Cargo.toml`:

```toml
[features]
default = []
wasm = ["wasm-bindgen", "js-sys", "getrandom/js"]

[dependencies]
# ... existing deps ...

# WASM bindings (optional)
wasm-bindgen = { version = "0.2", optional = true }
getrandom = { version = "0.2", optional = true }

[target.'cfg(target_family = "wasm")'.dependencies]
js-sys = "0.3"
```

**Step 2: Verify it compiles**

Run: `cargo check -p otlp2parquet-core --features wasm`
Expected: Compilation succeeds

**Step 3: Commit**

```bash
git add crates/otlp2parquet-core/Cargo.toml
git commit -m "feat(core): add wasm feature flag for browser builds"
```

---

## Task 2: Add WASM Bindings for Logs Conversion

**Files:**
- Modify: `crates/otlp2parquet-core/src/lib.rs`

**Step 1: Add wasm-bindgen exports for logs**

Add at the end of `crates/otlp2parquet-core/src/lib.rs`:

```rust
// WASM bindings for browser usage
#[cfg(feature = "wasm")]
pub mod wasm {
    use super::*;
    use wasm_bindgen::prelude::*;

    /// Convert OTLP JSON logs to Arrow IPC bytes
    ///
    /// Input: JSON string in OTLP ExportLogsServiceRequest format
    /// Output: Arrow IPC stream bytes (can be loaded by DuckDB)
    #[wasm_bindgen]
    pub fn logs_json_to_arrow_ipc(otlp_json: &str) -> Result<Vec<u8>, JsError> {
        let (batch, _metadata) = parse_otlp_to_arrow(otlp_json.as_bytes(), InputFormat::Json)
            .map_err(|e| JsError::new(&e.to_string()))?;

        batch_to_ipc_bytes(&batch).map_err(|e| JsError::new(&e.to_string()))
    }

    fn batch_to_ipc_bytes(batch: &RecordBatch) -> Result<Vec<u8>> {
        use arrow::ipc::writer::StreamWriter;
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
            writer.write(batch)?;
            writer.finish()?;
        }
        Ok(buf)
    }
}
```

**Step 2: Verify it compiles**

Run: `cargo check -p otlp2parquet-core --features wasm --target wasm32-unknown-unknown`
Expected: Compilation succeeds

**Step 3: Commit**

```bash
git add crates/otlp2parquet-core/src/lib.rs
git commit -m "feat(core): add WASM binding for logs JSON to Arrow IPC"
```

---

## Task 3: Add WASM Bindings for Traces Conversion

**Files:**
- Modify: `crates/otlp2parquet-core/src/lib.rs`
- Modify: `crates/otlp2parquet-core/src/otlp/mod.rs`

**Step 1: Re-export traces converter in otlp/mod.rs**

In `crates/otlp2parquet-core/src/otlp/mod.rs`, add:

```rust
pub use traces::{
    parse_otlp_request as parse_traces_request,
    ArrowConverter as TracesArrowConverter,
    TraceMetadata
};
```

**Step 2: Add traces WASM binding**

Add to the `wasm` module in `crates/otlp2parquet-core/src/lib.rs`:

```rust
    /// Convert OTLP JSON traces to Arrow IPC bytes
    #[wasm_bindgen]
    pub fn traces_json_to_arrow_ipc(otlp_json: &str) -> Result<Vec<u8>, JsError> {
        use otlp::traces;

        let request = traces::parse_otlp_request(otlp_json.as_bytes(), InputFormat::Json)
            .map_err(|e| JsError::new(&e.to_string()))?;

        let mut converter = traces::ArrowConverter::new();
        converter.add_from_request(&request)
            .map_err(|e| JsError::new(&e.to_string()))?;
        let (batch, _metadata) = converter.finish()
            .map_err(|e| JsError::new(&e.to_string()))?;

        batch_to_ipc_bytes(&batch).map_err(|e| JsError::new(&e.to_string()))
    }
```

**Step 3: Verify it compiles**

Run: `cargo check -p otlp2parquet-core --features wasm --target wasm32-unknown-unknown`
Expected: Compilation succeeds

**Step 4: Commit**

```bash
git add crates/otlp2parquet-core/src/lib.rs crates/otlp2parquet-core/src/otlp/mod.rs
git commit -m "feat(core): add WASM binding for traces JSON to Arrow IPC"
```

---

## Task 4: Add WASM Bindings for Metrics Conversion

**Files:**
- Modify: `crates/otlp2parquet-core/src/lib.rs`
- Modify: `crates/otlp2parquet-core/src/otlp/mod.rs`

**Step 1: Re-export metrics converter in otlp/mod.rs**

In `crates/otlp2parquet-core/src/otlp/mod.rs`, add:

```rust
pub use metrics::{
    parse_otlp_request as parse_metrics_request,
    ArrowConverter as MetricsArrowConverter,
    MetricsMetadata
};
```

**Step 2: Add metrics WASM binding**

Add to the `wasm` module in `crates/otlp2parquet-core/src/lib.rs`:

```rust
    /// Convert OTLP JSON metrics to Arrow IPC bytes (returns gauge data only for demo)
    #[wasm_bindgen]
    pub fn metrics_json_to_arrow_ipc(otlp_json: &str) -> Result<Vec<u8>, JsError> {
        use otlp::metrics;

        let request = metrics::parse_otlp_request(otlp_json.as_bytes(), InputFormat::Json)
            .map_err(|e| JsError::new(&e.to_string()))?;

        let mut converter = metrics::ArrowConverter::new();
        converter.add_from_request(&request)
            .map_err(|e| JsError::new(&e.to_string()))?;
        let batches = converter.finish()
            .map_err(|e| JsError::new(&e.to_string()))?;

        // Return gauge batch if available, otherwise empty
        if let Some((batch, _)) = batches.gauge {
            batch_to_ipc_bytes(&batch).map_err(|e| JsError::new(&e.to_string()))
        } else {
            // Return empty batch with gauge schema
            let schema = crate::schema::otel_metrics_gauge_schema();
            let empty = RecordBatch::new_empty(std::sync::Arc::new(schema));
            batch_to_ipc_bytes(&empty).map_err(|e| JsError::new(&e.to_string()))
        }
    }
```

**Step 3: Re-export gauge schema**

In `crates/otlp2parquet-core/src/lib.rs` at the top, add to re-exports:

```rust
pub use schema::otel_metrics_gauge_schema;
```

**Step 4: Verify it compiles**

Run: `cargo check -p otlp2parquet-core --features wasm --target wasm32-unknown-unknown`
Expected: Compilation succeeds

**Step 5: Commit**

```bash
git add crates/otlp2parquet-core/src/lib.rs crates/otlp2parquet-core/src/otlp/mod.rs
git commit -m "feat(core): add WASM binding for metrics JSON to Arrow IPC"
```

---

## Task 5: Add wasm-demo Make Target

**Files:**
- Modify: `Makefile`

**Step 1: Add wasm-demo target**

Add after the `wasm-full` target in `Makefile`:

```makefile
#
# Demo WASM Build
#

WASM_DEMO_OUT := docs/query-demo/wasm

.PHONY: wasm-demo
wasm-demo: ## Build otlp2parquet-core WASM for browser demo
	@echo "==> Building otlp2parquet-core WASM for browser demo..."
	@if ! command -v wasm-pack >/dev/null 2>&1; then \
		echo "Installing wasm-pack..."; \
		cargo install wasm-pack; \
	fi
	@wasm-pack build crates/otlp2parquet-core \
		--target web \
		--out-dir ../../$(WASM_DEMO_OUT) \
		--features wasm \
		--release
	@echo "==> WASM demo built to $(WASM_DEMO_OUT)/"
	@SIZE=$$(stat -f%z $(WASM_DEMO_OUT)/otlp2parquet_core_bg.wasm 2>/dev/null || stat -c%s $(WASM_DEMO_OUT)/otlp2parquet_core_bg.wasm 2>/dev/null); \
	python3 -c "import sys; size=int(sys.argv[1]); print(f\"==> WASM size: {size/1024:.1f} KB ({size/1024/1024:.3f} MB)\")" $$SIZE

.PHONY: clean-wasm-demo
clean-wasm-demo: ## Remove demo WASM artifacts
	@rm -rf $(WASM_DEMO_OUT)
	@echo "Cleaned demo WASM artifacts"
```

**Step 2: Add wasm/ to .gitignore**

Check if `docs/query-demo/wasm/` should be gitignored. Add to `.gitignore`:

```
# WASM demo build output
docs/query-demo/wasm/
```

**Step 3: Test the build**

Run: `make wasm-demo`
Expected: WASM files created in `docs/query-demo/wasm/`

**Step 4: Commit**

```bash
git add Makefile .gitignore
git commit -m "build: add wasm-demo make target for browser demo"
```

---

## Task 6: Create New Demo HTML Layout

**Files:**
- Modify: `docs/query-demo/index.html`

**Step 1: Replace index.html with new split-panel layout**

Replace entire contents of `docs/query-demo/index.html`:

```html
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Live Browser Observability Demo</title>
    <link rel="stylesheet" href="style.css" />
  </head>
  <body>
    <header class="header">
      <h1>Live Browser Observability</h1>
      <p>OTel JS SDK → otlp2parquet WASM → DuckDB → Perspective</p>
      <div class="status" id="status">
        <span id="status-text">Initializing...</span>
      </div>
    </header>

    <main class="main">
      <!-- Left Panel: SQL Query -->
      <section class="panel panel--sql">
        <h2>SQL Query</h2>
        <div class="sql-examples">
          <button class="example" data-query="SELECT Timestamp, SeverityText, Body FROM logs ORDER BY Timestamp DESC LIMIT 20">Recent Logs</button>
          <button class="example" data-query="SELECT Timestamp, MetricName, Value FROM metrics_gauge ORDER BY Timestamp DESC LIMIT 20">Recent Metrics</button>
          <button class="example" data-query="SELECT Timestamp, SpanName, Duration/1000000.0 as duration_ms FROM traces ORDER BY Timestamp DESC LIMIT 20">Recent Traces</button>
        </div>
        <textarea id="sql" spellcheck="false">SELECT Timestamp, SeverityText, Body
FROM logs
ORDER BY Timestamp DESC
LIMIT 20</textarea>
        <div class="actions">
          <button id="run-query" disabled>Run Query</button>
        </div>
        <div class="query-results">
          <h3>Query Results</h3>
          <perspective-viewer id="query-viewer"></perspective-viewer>
        </div>
      </section>

      <!-- Right Panel: Dashboard -->
      <section class="panel panel--dashboard">
        <div class="dashboard-grid">
          <!-- Logs Widget -->
          <div class="widget widget--logs">
            <h3>Logs <span class="widget-count" id="logs-count">0</span></h3>
            <perspective-viewer id="logs-viewer"></perspective-viewer>
          </div>

          <!-- Metrics Widget -->
          <div class="widget widget--metrics">
            <h3>Metrics <span class="widget-count" id="metrics-count">0</span></h3>
            <perspective-viewer id="metrics-viewer"></perspective-viewer>
          </div>

          <!-- Traces Widget -->
          <div class="widget widget--traces">
            <h3>Traces <span class="widget-count" id="traces-count">0</span></h3>
            <perspective-viewer id="traces-viewer"></perspective-viewer>
          </div>

          <!-- Playground -->
          <div class="widget widget--playground">
            <h3>Interactive Playground</h3>
            <p>Click buttons to generate telemetry:</p>
            <div class="playground-buttons">
              <button id="btn-log">Log Message</button>
              <button id="btn-error">Throw Error</button>
              <button id="btn-slow-api">Slow API (2s)</button>
              <button id="btn-nested">Nested Spans</button>
            </div>
          </div>
        </div>
      </section>
    </main>

    <footer class="footer">
      <p>
        Powered by <a href="https://github.com/smithclay/otlp2parquet">otlp2parquet</a> |
        <a href="https://perspective.finos.org/">Perspective</a> |
        <a href="https://duckdb.org/docs/api/wasm/overview">DuckDB-WASM</a>
      </p>
    </footer>

    <script type="module" src="app.js"></script>
  </body>
</html>
```

**Step 2: Verify HTML is valid**

Open in browser (will be broken until JS/CSS updated)

**Step 3: Commit**

```bash
git add docs/query-demo/index.html
git commit -m "feat(demo): add split-panel layout with Perspective viewers"
```

---

## Task 7: Update Demo CSS for Dashboard Layout

**Files:**
- Modify: `docs/query-demo/style.css`

**Step 1: Replace style.css with new dashboard styles**

Replace entire contents of `docs/query-demo/style.css`:

```css
:root {
  --bg-dark: #0f172a;
  --bg-card: rgba(15, 23, 42, 0.85);
  --border-color: rgba(148, 163, 184, 0.15);
  --text-primary: #f8fafc;
  --text-secondary: rgba(226, 232, 240, 0.88);
  --accent-blue: #6366f1;
  --accent-green: #22c55e;
  --accent-red: #ef4444;
}

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  font-family: "Inter", system-ui, sans-serif;
  background: linear-gradient(180deg, #0f172a 0%, #111827 100%);
  color: var(--text-primary);
  min-height: 100vh;
}

/* Header */
.header {
  text-align: center;
  padding: 1.5rem 1rem;
  border-bottom: 1px solid var(--border-color);
}

.header h1 {
  margin: 0 0 0.5rem;
  font-size: 1.75rem;
}

.header p {
  margin: 0 0 1rem;
  color: var(--text-secondary);
  font-size: 0.9rem;
}

.status {
  display: inline-block;
  padding: 0.5rem 1rem;
  border-radius: 999px;
  font-weight: 600;
  font-size: 0.85rem;
  background: rgba(14, 165, 233, 0.18);
  border: 1px solid rgba(14, 165, 233, 0.4);
  color: #e0f2fe;
}

.status.status--success {
  background: rgba(34, 197, 94, 0.18);
  border-color: rgba(74, 222, 128, 0.4);
  color: #bbf7d0;
}

.status.status--error {
  background: rgba(239, 68, 68, 0.16);
  border-color: rgba(248, 113, 113, 0.35);
  color: #fecaca;
}

/* Main Layout */
.main {
  display: grid;
  grid-template-columns: 350px 1fr;
  gap: 1rem;
  padding: 1rem;
  min-height: calc(100vh - 150px);
}

/* Panels */
.panel {
  background: var(--bg-card);
  border: 1px solid var(--border-color);
  border-radius: 12px;
  padding: 1rem;
  overflow: hidden;
  display: flex;
  flex-direction: column;
}

.panel h2 {
  margin: 0 0 1rem;
  font-size: 1.1rem;
}

/* SQL Panel */
.panel--sql {
  display: flex;
  flex-direction: column;
}

.sql-examples {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
  margin-bottom: 0.75rem;
}

.example {
  padding: 0.4rem 0.75rem;
  border: none;
  border-radius: 999px;
  background: rgba(96, 165, 250, 0.16);
  color: #bfdbfe;
  font-size: 0.8rem;
  font-weight: 600;
  cursor: pointer;
  transition: background 0.2s;
}

.example:hover:not(:disabled) {
  background: rgba(96, 165, 250, 0.28);
}

.example:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

#sql {
  width: 100%;
  min-height: 120px;
  padding: 0.75rem;
  border-radius: 8px;
  border: 1px solid var(--border-color);
  background: rgba(15, 23, 42, 0.75);
  color: var(--text-primary);
  font-family: "JetBrains Mono", monospace;
  font-size: 0.85rem;
  resize: vertical;
}

#sql:focus {
  outline: none;
  border-color: var(--accent-blue);
}

.actions {
  margin: 0.75rem 0;
}

#run-query {
  padding: 0.6rem 1.5rem;
  border: none;
  border-radius: 999px;
  background: linear-gradient(135deg, #6366f1, #8b5cf6);
  color: white;
  font-weight: 700;
  cursor: pointer;
  transition: transform 0.2s, box-shadow 0.2s;
}

#run-query:hover:not(:disabled) {
  transform: translateY(-1px);
  box-shadow: 0 8px 20px rgba(99, 102, 241, 0.35);
}

#run-query:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.query-results {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-height: 200px;
}

.query-results h3 {
  margin: 0 0 0.5rem;
  font-size: 0.9rem;
  color: var(--text-secondary);
}

.query-results perspective-viewer {
  flex: 1;
  min-height: 150px;
}

/* Dashboard Panel */
.panel--dashboard {
  padding: 0.75rem;
}

.dashboard-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  grid-template-rows: 1fr 1fr auto;
  gap: 0.75rem;
  height: 100%;
}

.widget {
  background: rgba(30, 41, 59, 0.6);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 0.75rem;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.widget h3 {
  margin: 0 0 0.5rem;
  font-size: 0.9rem;
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.widget-count {
  font-size: 0.75rem;
  padding: 0.15rem 0.5rem;
  border-radius: 999px;
  background: rgba(99, 102, 241, 0.25);
  color: #a5b4fc;
}

.widget perspective-viewer {
  flex: 1;
  min-height: 150px;
}

.widget--traces {
  grid-column: span 2;
}

.widget--playground {
  grid-column: span 2;
  flex-direction: row;
  flex-wrap: wrap;
  align-items: center;
  gap: 1rem;
}

.widget--playground h3 {
  margin: 0;
}

.widget--playground p {
  margin: 0;
  color: var(--text-secondary);
  font-size: 0.85rem;
}

.playground-buttons {
  display: flex;
  gap: 0.5rem;
  flex-wrap: wrap;
}

.playground-buttons button {
  padding: 0.5rem 1rem;
  border: 1px solid var(--border-color);
  border-radius: 8px;
  background: rgba(30, 41, 59, 0.8);
  color: var(--text-primary);
  font-weight: 600;
  cursor: pointer;
  transition: background 0.2s, border-color 0.2s;
}

.playground-buttons button:hover {
  background: rgba(99, 102, 241, 0.2);
  border-color: var(--accent-blue);
}

#btn-error {
  border-color: rgba(248, 113, 113, 0.4);
}

#btn-error:hover {
  background: rgba(239, 68, 68, 0.2);
  border-color: var(--accent-red);
}

/* Footer */
.footer {
  text-align: center;
  padding: 1rem;
  color: var(--text-secondary);
  font-size: 0.85rem;
  border-top: 1px solid var(--border-color);
}

.footer a {
  color: #93c5fd;
}

/* Perspective Viewer Styling */
perspective-viewer {
  --plugin--background: transparent;
  --plugin--font-family: "Inter", system-ui, sans-serif;
}

/* Responsive */
@media (max-width: 900px) {
  .main {
    grid-template-columns: 1fr;
  }

  .dashboard-grid {
    grid-template-columns: 1fr;
  }

  .widget--traces,
  .widget--playground {
    grid-column: span 1;
  }
}
```

**Step 2: Commit**

```bash
git add docs/query-demo/style.css
git commit -m "feat(demo): update CSS for split-panel dashboard layout"
```

---

## Task 8: Create Telemetry Module (OTel SDK Setup)

**Files:**
- Create: `docs/query-demo/telemetry.js`

**Step 1: Create telemetry.js with OTel SDK setup**

Create `docs/query-demo/telemetry.js`:

```javascript
// OTel SDK setup with custom local exporter
// Captures browser telemetry and exports OTLP JSON to our WASM converter

import { WebTracerProvider } from 'https://cdn.jsdelivr.net/npm/@opentelemetry/sdk-trace-web@1.25.0/+esm';
import { SimpleSpanProcessor } from 'https://cdn.jsdelivr.net/npm/@opentelemetry/sdk-trace-base@1.25.0/+esm';
import { Resource } from 'https://cdn.jsdelivr.net/npm/@opentelemetry/resources@1.25.0/+esm';
import { SEMRESATTRS_SERVICE_NAME } from 'https://cdn.jsdelivr.net/npm/@opentelemetry/semantic-conventions@1.25.0/+esm';

// Callbacks for when telemetry is exported
let onLogsExport = null;
let onTracesExport = null;
let onMetricsExport = null;

// Tracer instance
let tracer = null;

// Logs buffer (console intercept)
const logsBuffer = [];
const LOGS_FLUSH_INTERVAL = 1000;
const LOGS_FLUSH_SIZE = 20;

/**
 * Custom span exporter that converts spans to OTLP JSON
 */
class LocalSpanExporter {
  export(spans, resultCallback) {
    if (spans.length === 0) {
      resultCallback({ code: 0 });
      return;
    }

    const otlpJson = this.spansToOtlpJson(spans);

    if (onTracesExport) {
      onTracesExport(otlpJson);
    }

    resultCallback({ code: 0 });
  }

  shutdown() {
    return Promise.resolve();
  }

  spansToOtlpJson(spans) {
    // Group spans by resource
    const resourceSpans = {};

    for (const span of spans) {
      const resourceKey = JSON.stringify(span.resource?.attributes || {});
      if (!resourceSpans[resourceKey]) {
        resourceSpans[resourceKey] = {
          resource: {
            attributes: this.attributesToOtlp(span.resource?.attributes || {})
          },
          scopeSpans: [{
            scope: {
              name: span.instrumentationLibrary?.name || 'browser-demo',
              version: span.instrumentationLibrary?.version || '1.0.0'
            },
            spans: []
          }]
        };
      }

      resourceSpans[resourceKey].scopeSpans[0].spans.push({
        traceId: span.spanContext().traceId,
        spanId: span.spanContext().spanId,
        parentSpanId: span.parentSpanId || '',
        name: span.name,
        kind: span.kind || 1,
        startTimeUnixNano: String(span.startTime[0] * 1e9 + span.startTime[1]),
        endTimeUnixNano: String(span.endTime[0] * 1e9 + span.endTime[1]),
        attributes: this.attributesToOtlp(span.attributes || {}),
        events: (span.events || []).map(e => ({
          timeUnixNano: String(e.time[0] * 1e9 + e.time[1]),
          name: e.name,
          attributes: this.attributesToOtlp(e.attributes || {})
        })),
        status: {
          code: span.status?.code || 0,
          message: span.status?.message || ''
        }
      });
    }

    return JSON.stringify({
      resourceSpans: Object.values(resourceSpans)
    });
  }

  attributesToOtlp(attrs) {
    return Object.entries(attrs).map(([key, value]) => ({
      key,
      value: this.valueToOtlp(value)
    }));
  }

  valueToOtlp(value) {
    if (typeof value === 'string') return { stringValue: value };
    if (typeof value === 'number') {
      return Number.isInteger(value) ? { intValue: String(value) } : { doubleValue: value };
    }
    if (typeof value === 'boolean') return { boolValue: value };
    if (Array.isArray(value)) return { arrayValue: { values: value.map(v => this.valueToOtlp(v)) } };
    return { stringValue: String(value) };
  }
}

/**
 * Initialize OpenTelemetry tracing
 */
export function initTracing() {
  const resource = new Resource({
    [SEMRESATTRS_SERVICE_NAME]: 'browser-demo'
  });

  const provider = new WebTracerProvider({ resource });
  provider.addSpanProcessor(new SimpleSpanProcessor(new LocalSpanExporter()));
  provider.register();

  tracer = provider.getTracer('browser-demo', '1.0.0');
  return tracer;
}

/**
 * Get the tracer instance
 */
export function getTracer() {
  return tracer;
}

/**
 * Intercept console methods to capture logs
 */
export function initLogsCapture() {
  const originalConsole = {
    log: console.log.bind(console),
    info: console.info.bind(console),
    warn: console.warn.bind(console),
    error: console.error.bind(console),
    debug: console.debug.bind(console)
  };

  const severityMap = {
    debug: { text: 'DEBUG', number: 5 },
    log: { text: 'INFO', number: 9 },
    info: { text: 'INFO', number: 9 },
    warn: { text: 'WARN', number: 13 },
    error: { text: 'ERROR', number: 17 }
  };

  for (const [method, severity] of Object.entries(severityMap)) {
    console[method] = (...args) => {
      originalConsole[method](...args);

      const body = args.map(arg =>
        typeof arg === 'object' ? JSON.stringify(arg) : String(arg)
      ).join(' ');

      logsBuffer.push({
        timeUnixNano: String(Date.now() * 1e6),
        severityNumber: severity.number,
        severityText: severity.text,
        body: { stringValue: body },
        attributes: [
          { key: 'source', value: { stringValue: 'console' } },
          { key: 'method', value: { stringValue: method } }
        ]
      });

      if (logsBuffer.length >= LOGS_FLUSH_SIZE) {
        flushLogs();
      }
    };
  }

  // Capture uncaught errors
  window.addEventListener('error', (event) => {
    logsBuffer.push({
      timeUnixNano: String(Date.now() * 1e6),
      severityNumber: 17,
      severityText: 'ERROR',
      body: { stringValue: `${event.message} at ${event.filename}:${event.lineno}` },
      attributes: [
        { key: 'source', value: { stringValue: 'window.onerror' } },
        { key: 'error.type', value: { stringValue: 'uncaught' } }
      ]
    });
    flushLogs();
  });

  // Capture unhandled promise rejections
  window.addEventListener('unhandledrejection', (event) => {
    logsBuffer.push({
      timeUnixNano: String(Date.now() * 1e6),
      severityNumber: 17,
      severityText: 'ERROR',
      body: { stringValue: `Unhandled rejection: ${event.reason}` },
      attributes: [
        { key: 'source', value: { stringValue: 'unhandledrejection' } }
      ]
    });
    flushLogs();
  });

  // Periodic flush
  setInterval(flushLogs, LOGS_FLUSH_INTERVAL);
}

function flushLogs() {
  if (logsBuffer.length === 0 || !onLogsExport) return;

  const otlpJson = JSON.stringify({
    resourceLogs: [{
      resource: {
        attributes: [
          { key: 'service.name', value: { stringValue: 'browser-demo' } }
        ]
      },
      scopeLogs: [{
        scope: { name: 'console-interceptor', version: '1.0.0' },
        logRecords: logsBuffer.splice(0, logsBuffer.length)
      }]
    }]
  });

  onLogsExport(otlpJson);
}

/**
 * Initialize metrics capture (Web Vitals style)
 */
export function initMetricsCapture() {
  // Capture performance metrics periodically
  setInterval(() => {
    const metrics = [];
    const now = Date.now() * 1e6;

    // Memory (if available)
    if (performance.memory) {
      metrics.push({
        name: 'browser.memory.used',
        description: 'JS heap used bytes',
        unit: 'bytes',
        gauge: {
          dataPoints: [{
            timeUnixNano: String(now),
            asDouble: performance.memory.usedJSHeapSize,
            attributes: []
          }]
        }
      });
    }

    // DOM nodes
    metrics.push({
      name: 'browser.dom.nodes',
      description: 'Number of DOM nodes',
      unit: '{nodes}',
      gauge: {
        dataPoints: [{
          timeUnixNano: String(now),
          asDouble: document.getElementsByTagName('*').length,
          attributes: []
        }]
      }
    });

    if (metrics.length > 0 && onMetricsExport) {
      const otlpJson = JSON.stringify({
        resourceMetrics: [{
          resource: {
            attributes: [
              { key: 'service.name', value: { stringValue: 'browser-demo' } }
            ]
          },
          scopeMetrics: [{
            scope: { name: 'browser-metrics', version: '1.0.0' },
            metrics
          }]
        }]
      });
      onMetricsExport(otlpJson);
    }
  }, 2000);
}

/**
 * Set callback for when logs are exported
 */
export function setOnLogsExport(callback) {
  onLogsExport = callback;
}

/**
 * Set callback for when traces are exported
 */
export function setOnTracesExport(callback) {
  onTracesExport = callback;
}

/**
 * Set callback for when metrics are exported
 */
export function setOnMetricsExport(callback) {
  onMetricsExport = callback;
}
```

**Step 2: Commit**

```bash
git add docs/query-demo/telemetry.js
git commit -m "feat(demo): add telemetry module with OTel SDK and console intercept"
```

---

## Task 9: Create Playground Module

**Files:**
- Create: `docs/query-demo/playground.js`

**Step 1: Create playground.js with interactive buttons**

Create `docs/query-demo/playground.js`:

```javascript
// Interactive playground for generating telemetry

import { getTracer } from './telemetry.js';

/**
 * Initialize playground button handlers
 */
export function initPlayground() {
  document.getElementById('btn-log').addEventListener('click', handleLogMessage);
  document.getElementById('btn-error').addEventListener('click', handleThrowError);
  document.getElementById('btn-slow-api').addEventListener('click', handleSlowApi);
  document.getElementById('btn-nested').addEventListener('click', handleNestedSpans);
}

function handleLogMessage() {
  const messages = [
    'User clicked the log button',
    'Processing user interaction',
    'Button click event handled successfully',
    'Demo log message generated'
  ];
  const msg = messages[Math.floor(Math.random() * messages.length)];
  console.log(`[Playground] ${msg}`);
}

function handleThrowError() {
  console.error('[Playground] About to throw an error...');
  setTimeout(() => {
    throw new Error('User-triggered error from playground');
  }, 100);
}

async function handleSlowApi() {
  const tracer = getTracer();
  if (!tracer) {
    console.warn('Tracer not initialized');
    return;
  }

  const span = tracer.startSpan('playground.slow-api-call');
  span.setAttribute('http.method', 'GET');
  span.setAttribute('http.url', '/api/slow');

  console.info('[Playground] Starting slow API call...');

  const delay = 2000 + Math.random() * 1000;
  await new Promise(resolve => setTimeout(resolve, delay));

  span.setAttribute('http.status_code', 200);
  span.setAttribute('http.response_time_ms', delay);
  span.end();

  console.info(`[Playground] Slow API call completed in ${delay.toFixed(0)}ms`);
}

async function handleNestedSpans() {
  const tracer = getTracer();
  if (!tracer) {
    console.warn('Tracer not initialized');
    return;
  }

  console.info('[Playground] Starting nested operations...');

  const parentSpan = tracer.startSpan('playground.parent-operation');
  parentSpan.setAttribute('operation.type', 'batch');

  for (let i = 1; i <= 3; i++) {
    const childSpan = tracer.startSpan(`playground.child-operation-${i}`, {
      attributes: { 'operation.index': i }
    });

    const delay = 200 + Math.random() * 300;
    await new Promise(resolve => setTimeout(resolve, delay));

    childSpan.setAttribute('operation.duration_ms', delay);
    childSpan.end();

    console.log(`[Playground] Child operation ${i} completed`);
  }

  parentSpan.end();
  console.info('[Playground] All nested operations completed');
}
```

**Step 2: Commit**

```bash
git add docs/query-demo/playground.js
git commit -m "feat(demo): add playground module with interactive buttons"
```

---

## Task 10: Create Main App Orchestrator

**Files:**
- Modify: `docs/query-demo/app.js`

**Step 1: Replace app.js with new orchestrator**

Replace entire contents of `docs/query-demo/app.js`:

```javascript
// Main application orchestrator
// Initializes DuckDB, WASM converter, OTel SDK, and Perspective viewers

import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';
import perspective from 'https://cdn.jsdelivr.net/npm/@finos/perspective@2.10.1/dist/esm/perspective.js';

import {
  initTracing,
  initLogsCapture,
  initMetricsCapture,
  setOnLogsExport,
  setOnTracesExport,
  setOnMetricsExport
} from './telemetry.js';
import { initPlayground } from './playground.js';

// Global state
let db = null;
let conn = null;
let wasm = null;
let perspectiveWorker = null;

// Perspective tables for streaming updates
let logsTable = null;
let tracesTable = null;
let metricsTable = null;

// Counters
let logsCount = 0;
let tracesCount = 0;
let metricsCount = 0;

// DOM elements
const statusEl = document.getElementById('status');
const statusTextEl = document.getElementById('status-text');
const runButton = document.getElementById('run-query');
const sqlInput = document.getElementById('sql');
const exampleButtons = document.querySelectorAll('.example');

// Status helpers
function updateStatus(message, variant = 'info') {
  statusEl.classList.remove('status--success', 'status--error');
  if (variant === 'success') statusEl.classList.add('status--success');
  if (variant === 'error') statusEl.classList.add('status--error');
  statusTextEl.textContent = message;
}

// DuckDB initialization
async function initDuckDB() {
  updateStatus('Loading DuckDB...');

  const bundle = await duckdb.selectBundle({
    mvp: {
      mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist/duckdb-mvp.wasm',
      mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist/duckdb-browser-mvp.worker.js'
    },
    eh: {
      mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist/duckdb-eh.wasm',
      mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist/duckdb-browser-eh.worker.js'
    }
  });

  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
  );
  const worker = new Worker(workerUrl);
  const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);

  db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule);
  URL.revokeObjectURL(workerUrl);

  conn = await db.connect();

  // Create tables with otlp2parquet schemas
  await conn.query(`
    CREATE TABLE logs (
      Timestamp TIMESTAMP,
      TraceId VARCHAR,
      SpanId VARCHAR,
      ServiceName VARCHAR,
      SeverityText VARCHAR,
      SeverityNumber INTEGER,
      Body VARCHAR,
      LogAttributes VARCHAR
    )
  `);

  await conn.query(`
    CREATE TABLE traces (
      Timestamp TIMESTAMP,
      TraceId VARCHAR,
      SpanId VARCHAR,
      ParentSpanId VARCHAR,
      ServiceName VARCHAR,
      SpanName VARCHAR,
      SpanKind VARCHAR,
      Duration BIGINT,
      StatusCode VARCHAR,
      SpanAttributes VARCHAR
    )
  `);

  await conn.query(`
    CREATE TABLE metrics_gauge (
      Timestamp TIMESTAMP,
      ServiceName VARCHAR,
      MetricName VARCHAR,
      MetricDescription VARCHAR,
      MetricUnit VARCHAR,
      Value DOUBLE,
      Attributes VARCHAR
    )
  `);
}

// WASM initialization
async function initWasm() {
  updateStatus('Loading WASM converter...');

  try {
    const wasmModule = await import('./wasm/otlp2parquet_core.js');
    await wasmModule.default();
    wasm = wasmModule;
    console.log('WASM converter loaded');
  } catch (e) {
    console.warn('WASM not available, using fallback JSON parsing:', e.message);
    wasm = null;
  }
}

// Perspective initialization
async function initPerspective() {
  updateStatus('Loading Perspective...');

  perspectiveWorker = await perspective.worker();

  // Create streaming tables
  logsTable = await perspectiveWorker.table({
    Timestamp: 'datetime',
    SeverityText: 'string',
    Body: 'string'
  });

  tracesTable = await perspectiveWorker.table({
    Timestamp: 'datetime',
    SpanName: 'string',
    Duration: 'float'
  });

  metricsTable = await perspectiveWorker.table({
    Timestamp: 'datetime',
    MetricName: 'string',
    Value: 'float'
  });

  // Configure viewers
  const logsViewer = document.getElementById('logs-viewer');
  await logsViewer.load(logsTable);
  await logsViewer.restore({
    plugin: 'Datagrid',
    columns: ['Timestamp', 'SeverityText', 'Body'],
    sort: [['Timestamp', 'desc']]
  });

  const tracesViewer = document.getElementById('traces-viewer');
  await tracesViewer.load(tracesTable);
  await tracesViewer.restore({
    plugin: 'Datagrid',
    columns: ['Timestamp', 'SpanName', 'Duration'],
    sort: [['Timestamp', 'desc']]
  });

  const metricsViewer = document.getElementById('metrics-viewer');
  await metricsViewer.load(metricsTable);
  await metricsViewer.restore({
    plugin: 'Y Line',
    group_by: ['MetricName'],
    columns: ['Value'],
    sort: [['Timestamp', 'asc']]
  });
}

// Telemetry handlers
async function handleLogsExport(otlpJson) {
  try {
    const data = JSON.parse(otlpJson);
    const rows = [];

    for (const resourceLogs of data.resourceLogs || []) {
      const serviceName = resourceLogs.resource?.attributes?.find(
        a => a.key === 'service.name'
      )?.value?.stringValue || 'unknown';

      for (const scopeLogs of resourceLogs.scopeLogs || []) {
        for (const log of scopeLogs.logRecords || []) {
          const ts = new Date(parseInt(log.timeUnixNano) / 1e6);
          rows.push({
            Timestamp: ts,
            SeverityText: log.severityText || 'INFO',
            Body: log.body?.stringValue || ''
          });

          // Insert into DuckDB
          const body = (log.body?.stringValue || '').replace(/'/g, "''");
          await conn.query(`
            INSERT INTO logs (Timestamp, ServiceName, SeverityText, SeverityNumber, Body)
            VALUES ('${ts.toISOString()}', '${serviceName}', '${log.severityText}', ${log.severityNumber}, '${body}')
          `);
        }
      }
    }

    if (rows.length > 0) {
      await logsTable.update(rows);
      logsCount += rows.length;
      document.getElementById('logs-count').textContent = logsCount;
    }
  } catch (e) {
    console.error('Failed to process logs:', e);
  }
}

async function handleTracesExport(otlpJson) {
  try {
    const data = JSON.parse(otlpJson);
    const rows = [];

    for (const resourceSpans of data.resourceSpans || []) {
      const serviceName = resourceSpans.resource?.attributes?.find(
        a => a.key === 'service.name'
      )?.value?.stringValue || 'browser-demo';

      for (const scopeSpans of resourceSpans.scopeSpans || []) {
        for (const span of scopeSpans.spans || []) {
          const startNano = BigInt(span.startTimeUnixNano);
          const endNano = BigInt(span.endTimeUnixNano);
          const durationMs = Number(endNano - startNano) / 1e6;
          const ts = new Date(Number(startNano / BigInt(1e6)));

          rows.push({
            Timestamp: ts,
            SpanName: span.name,
            Duration: durationMs
          });

          // Insert into DuckDB
          await conn.query(`
            INSERT INTO traces (Timestamp, TraceId, SpanId, ParentSpanId, ServiceName, SpanName, Duration, StatusCode)
            VALUES ('${ts.toISOString()}', '${span.traceId}', '${span.spanId}', '${span.parentSpanId || ''}',
                    '${serviceName}', '${span.name}', ${Math.round(durationMs * 1e6)}, '${span.status?.code || 0}')
          `);
        }
      }
    }

    if (rows.length > 0) {
      await tracesTable.update(rows);
      tracesCount += rows.length;
      document.getElementById('traces-count').textContent = tracesCount;
    }
  } catch (e) {
    console.error('Failed to process traces:', e);
  }
}

async function handleMetricsExport(otlpJson) {
  try {
    const data = JSON.parse(otlpJson);
    const rows = [];

    for (const resourceMetrics of data.resourceMetrics || []) {
      const serviceName = resourceMetrics.resource?.attributes?.find(
        a => a.key === 'service.name'
      )?.value?.stringValue || 'browser-demo';

      for (const scopeMetrics of resourceMetrics.scopeMetrics || []) {
        for (const metric of scopeMetrics.metrics || []) {
          const dataPoints = metric.gauge?.dataPoints || [];
          for (const dp of dataPoints) {
            const ts = new Date(parseInt(dp.timeUnixNano) / 1e6);
            rows.push({
              Timestamp: ts,
              MetricName: metric.name,
              Value: dp.asDouble
            });

            // Insert into DuckDB
            await conn.query(`
              INSERT INTO metrics_gauge (Timestamp, ServiceName, MetricName, MetricDescription, MetricUnit, Value)
              VALUES ('${ts.toISOString()}', '${serviceName}', '${metric.name}',
                      '${metric.description || ''}', '${metric.unit || ''}', ${dp.asDouble})
            `);
          }
        }
      }
    }

    if (rows.length > 0) {
      await metricsTable.update(rows);
      metricsCount += rows.length;
      document.getElementById('metrics-count').textContent = metricsCount;
    }
  } catch (e) {
    console.error('Failed to process metrics:', e);
  }
}

// SQL query execution
async function runQuery() {
  const sql = sqlInput.value.trim();
  if (!sql) return;

  try {
    updateStatus('Running query...');
    const result = await conn.query(sql);
    const rows = result.toArray();

    if (rows.length === 0) {
      updateStatus('Query returned 0 rows', 'success');
      return;
    }

    // Get column names from schema
    const columns = result.schema.fields.map(f => f.name);

    // Create schema for Perspective
    const schema = {};
    for (const field of result.schema.fields) {
      const type = field.type.toString();
      if (type.includes('Int') || type.includes('Float') || type.includes('Double')) {
        schema[field.name] = 'float';
      } else if (type.includes('Timestamp') || type.includes('Date')) {
        schema[field.name] = 'datetime';
      } else {
        schema[field.name] = 'string';
      }
    }

    // Convert rows to objects
    const data = rows.map(row => {
      const obj = {};
      for (const col of columns) {
        let val = row[col];
        // Handle BigInt
        if (typeof val === 'bigint') val = Number(val);
        // Handle timestamps
        if (val instanceof Date) val = val;
        obj[col] = val;
      }
      return obj;
    });

    // Update query viewer
    const queryViewer = document.getElementById('query-viewer');
    const queryTable = await perspectiveWorker.table(schema);
    await queryTable.update(data);
    await queryViewer.load(queryTable);
    await queryViewer.restore({
      plugin: 'Datagrid',
      columns: columns
    });

    updateStatus(`Query returned ${rows.length} rows`, 'success');
  } catch (e) {
    console.error('Query failed:', e);
    updateStatus(`Query error: ${e.message}`, 'error');
  }
}

// Initialize everything
async function init() {
  try {
    await initDuckDB();
    await initWasm();
    await initPerspective();

    // Set up telemetry export handlers
    setOnLogsExport(handleLogsExport);
    setOnTracesExport(handleTracesExport);
    setOnMetricsExport(handleMetricsExport);

    // Initialize telemetry collection
    updateStatus('Starting telemetry collection...');
    initTracing();
    initLogsCapture();
    initMetricsCapture();

    // Initialize playground
    initPlayground();

    // Enable UI
    runButton.disabled = false;
    runButton.addEventListener('click', runQuery);

    sqlInput.addEventListener('keydown', (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') runQuery();
    });

    exampleButtons.forEach(btn => {
      btn.disabled = false;
      btn.addEventListener('click', () => {
        sqlInput.value = btn.dataset.query;
      });
    });

    updateStatus('Ready! Telemetry is being captured.', 'success');

    // Log startup
    console.info('[Demo] Live Browser Observability Demo initialized');
    console.log('[Demo] Try clicking the playground buttons to generate telemetry');

  } catch (e) {
    console.error('Initialization failed:', e);
    updateStatus(`Failed to initialize: ${e.message}`, 'error');
  }
}

// Custom elements for Perspective
import 'https://cdn.jsdelivr.net/npm/@finos/perspective-viewer@2.10.1/dist/esm/perspective-viewer.js';
import 'https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-datagrid@2.10.1/dist/esm/perspective-viewer-datagrid.js';
import 'https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-d3fc@2.10.1/dist/esm/perspective-viewer-d3fc.js';

init();
```

**Step 2: Commit**

```bash
git add docs/query-demo/app.js
git commit -m "feat(demo): add main orchestrator with DuckDB, Perspective, and telemetry"
```

---

## Task 11: Test the Demo Locally

**Step 1: Build the WASM**

Run: `make wasm-demo`
Expected: WASM files created in `docs/query-demo/wasm/`

**Step 2: Serve the demo**

Run: `cd docs/query-demo && python3 -m http.server 8080`

**Step 3: Open in browser**

Navigate to: `http://localhost:8080`
Expected: Demo loads, shows "Ready!" status

**Step 4: Test playground buttons**

Click each button and verify:
- "Log Message" → logs appear in logs widget
- "Throw Error" → error log appears
- "Slow API" → trace span appears after 2-3s
- "Nested Spans" → multiple trace spans appear

**Step 5: Test SQL queries**

Click example buttons and run queries
Expected: Query results appear in Perspective viewer

---

## Task 12: Fix Any Issues and Final Commit

**Step 1: Fix any bugs found during testing**

Review console for errors, fix as needed

**Step 2: Final commit**

```bash
git add -A
git commit -m "feat(demo): complete live browser observability demo"
```

---

## Summary

**Tasks:**
1. Add WASM feature flag to Cargo.toml
2. Add WASM bindings for logs conversion
3. Add WASM bindings for traces conversion
4. Add WASM bindings for metrics conversion
5. Add wasm-demo Make target
6. Create new HTML layout
7. Update CSS for dashboard
8. Create telemetry module (OTel SDK)
9. Create playground module
10. Create main app orchestrator
11. Test locally
12. Fix issues and final commit

**Key files created/modified:**
- `crates/otlp2parquet-core/Cargo.toml` - WASM feature
- `crates/otlp2parquet-core/src/lib.rs` - WASM bindings
- `Makefile` - wasm-demo target
- `docs/query-demo/index.html` - new layout
- `docs/query-demo/style.css` - dashboard styles
- `docs/query-demo/app.js` - main orchestrator
- `docs/query-demo/telemetry.js` - OTel SDK setup
- `docs/query-demo/playground.js` - interactive buttons
