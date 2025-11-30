// Main application orchestrator
// Initializes DuckDB, WASM converter, OTel SDK, and Perspective viewers

import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';
// Use CDN builds from /dist/cdn/ which have bundled WASM
import perspective from 'https://cdn.jsdelivr.net/npm/@finos/perspective@3.1.0/dist/cdn/perspective.js';

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
    // Explicitly fetch the WASM file to avoid MIME type issues
    const wasmUrl = new URL('./wasm/otlp2parquet_core_bg.wasm', window.location.href);
    await wasmModule.default(wasmUrl);
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

// Custom elements for Perspective (CDN builds from /dist/cdn/)
import 'https://cdn.jsdelivr.net/npm/@finos/perspective-viewer@3.1.0/dist/cdn/perspective-viewer.js';
import 'https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-datagrid@3.1.0/dist/cdn/perspective-viewer-datagrid.js';
import 'https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-d3fc@3.1.0/dist/cdn/perspective-viewer-d3fc.js';

init();
