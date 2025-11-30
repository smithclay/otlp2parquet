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
