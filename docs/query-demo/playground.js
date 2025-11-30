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
