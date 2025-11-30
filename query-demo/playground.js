// Interactive playground for generating telemetry

import { getTracer } from './telemetry.js';

/**
 * Initialize playground button handlers
 */
export function initPlayground() {
  // Original buttons
  document.getElementById('btn-log').addEventListener('click', handleLogMessage);
  document.getElementById('btn-error').addEventListener('click', handleThrowError);
  document.getElementById('btn-slow').addEventListener('click', handleSlowApi);
  document.getElementById('btn-nested').addEventListener('click', handleNestedSpans);

  // New stress test buttons
  document.getElementById('btn-dom-stress').addEventListener('click', handleDomStress);
  document.getElementById('btn-layout-shift').addEventListener('click', handleLayoutShift);
  document.getElementById('btn-long-task').addEventListener('click', handleLongTask);
}

function handleLogMessage() {
  const messages = [
    'User clicked the log button',
    'Processing user interaction',
    'Button click event handled successfully',
    'Demo log message generated',
    'Analytics event captured',
    'User engagement tracked'
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

/**
 * Add 100 DOM nodes to stress test DOM metrics
 */
function handleDomStress() {
  const tracer = getTracer();
  const span = tracer?.startSpan('playground.dom-stress');

  console.info('[Playground] Adding 100 DOM nodes...');

  const container = document.createElement('div');
  container.className = 'stress-test-container';
  container.style.cssText = 'position:fixed;bottom:10px;right:10px;max-width:200px;max-height:150px;overflow:auto;background:rgba(0,0,0,0.8);padding:8px;border-radius:8px;font-size:10px;z-index:1000;';

  for (let i = 0; i < 100; i++) {
    const el = document.createElement('span');
    el.textContent = `N${i} `;
    el.style.color = `hsl(${(i * 3.6) % 360}, 70%, 60%)`;
    container.appendChild(el);
  }

  // Add close button
  const closeBtn = document.createElement('button');
  closeBtn.textContent = 'X';
  closeBtn.style.cssText = 'position:absolute;top:2px;right:2px;background:red;color:white;border:none;border-radius:4px;cursor:pointer;font-size:10px;padding:2px 6px;';
  closeBtn.onclick = () => {
    container.remove();
    console.log('[Playground] Stress test container removed');
  };
  container.style.position = 'fixed';
  container.appendChild(closeBtn);

  document.body.appendChild(container);

  span?.setAttribute('nodes.added', 100);
  span?.end();

  console.info('[Playground] Added 100 DOM nodes (click X to remove)');
}

/**
 * Trigger a layout shift by dynamically inserting content
 */
function handleLayoutShift() {
  const tracer = getTracer();
  const span = tracer?.startSpan('playground.layout-shift');

  console.warn('[Playground] Triggering layout shift...');

  // Find a good place to insert - header area
  const header = document.querySelector('.header');
  if (!header) {
    console.error('[Playground] Could not find header element');
    span?.end();
    return;
  }

  // Create a banner that pushes content down
  const banner = document.createElement('div');
  banner.className = 'layout-shift-banner';
  banner.style.cssText = 'background:linear-gradient(90deg,#ef4444,#f97316);color:white;padding:12px;text-align:center;font-weight:bold;animation:slideIn 0.3s ease-out;';
  banner.innerHTML = 'This banner caused a layout shift! <button onclick="this.parentElement.remove()" style="margin-left:10px;background:white;color:#ef4444;border:none;padding:4px 12px;border-radius:4px;cursor:pointer;font-weight:bold;">Dismiss</button>';

  // Insert at top of header (causes shift)
  header.insertBefore(banner, header.firstChild);

  span?.setAttribute('shift.type', 'banner-insertion');
  span?.end();

  console.warn('[Playground] Layout shift triggered - check CLS metric!');

  // Auto-remove after 5 seconds
  setTimeout(() => {
    if (banner.parentElement) {
      banner.remove();
      console.log('[Playground] Layout shift banner auto-removed');
    }
  }, 5000);
}

/**
 * Block the main thread for ~100ms to trigger long task detection
 */
function handleLongTask() {
  const tracer = getTracer();
  const span = tracer?.startSpan('playground.long-task');

  console.warn('[Playground] Blocking main thread for 100ms...');

  const start = performance.now();
  const blockTime = 100; // ms

  // Busy wait to block main thread
  while (performance.now() - start < blockTime) {
    // Intentionally blocking - doing meaningless work
    Math.sqrt(Math.random() * 1000000);
  }

  const elapsed = performance.now() - start;

  span?.setAttribute('block.duration_ms', elapsed);
  span?.end();

  console.warn(`[Playground] Main thread blocked for ${elapsed.toFixed(0)}ms - check Long Tasks metric!`);
}
