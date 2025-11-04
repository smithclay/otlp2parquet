import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.31.0/+esm';

const statusEl = document.getElementById('status');
const statusTextEl = document.getElementById('status-text');
const runButton = document.getElementById('run-query');
const sqlInput = document.getElementById('sql');
const exampleButtons = Array.from(document.querySelectorAll('.example'));
const resultError = document.getElementById('result-error');
const resultMeta = document.getElementById('result-meta');
const tableWrapper = document.getElementById('result-table-wrapper');
const resultTableHead = document.querySelector('#result-table thead');
const resultTableBody = document.querySelector('#result-table tbody');

let db;
let conn;

class SampleFilesMissingError extends Error {
  constructor(failures) {
    super('Sample Parquet files are missing');
    this.name = 'SampleFilesMissingError';
    this.failures = failures;
  }
}

exampleButtons.forEach((btn) => {
  btn.disabled = true;
});

function updateStatus(message, variant = 'info') {
  statusEl.classList.remove('status--success', 'status--error');
  if (variant === 'success') {
    statusEl.classList.add('status--success');
  } else if (variant === 'error') {
    statusEl.classList.add('status--error');
  }
  statusTextEl.textContent = message;
}

async function initDuckDB() {
  try {
    updateStatus('Loading DuckDB-Wasm…');
    const bundle = await duckdb.selectBundle({
      mvp: {
        mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.31.0/dist/duckdb-mvp.wasm',
        mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.31.0/dist/duckdb-browser-mvp.worker.js'
      },
      eh: {
        mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.31.0/dist/duckdb-eh.wasm',
        mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.31.0/dist/duckdb-browser-eh.worker.js'
      }
    });

    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
    );
    const worker = new Worker(workerUrl);
    const logger = new duckdb.ConsoleLogger();
    db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(workerUrl);

    conn = await db.connect();
    await conn.query(`SET autoinstall_known_extensions = true;`);
    updateStatus('Registering Parquet fixtures…');
    await registerSampleFiles();

    runButton.disabled = false;
    exampleButtons.forEach((btn) => {
      btn.disabled = false;
      btn.addEventListener('click', () => {
        sqlInput.value = btn.dataset.query;
        sqlInput.focus();
      });
    });

    updateStatus('Ready! Press “Run query” or try an example.', 'success');
  } catch (error) {
    console.error('Failed to initialise DuckDB', error);
    if (error instanceof SampleFilesMissingError) {
      updateStatus('Sample Parquet files missing. Generate them and refresh.', 'error');
      const missingItems = error.failures
        .map((failure) => `<li><code>${failure.sample.url}</code></li>`)
        .join('');
      runButton.disabled = true;
      exampleButtons.forEach((btn) => {
        btn.disabled = true;
      });
      tableWrapper.classList.add('hidden');
      resultMeta.textContent = '';
      resultError.innerHTML = `
        <p>Sample Parquet files could not be loaded.</p>
        <p>Generate them by running:</p>
        <pre><code>cargo run -p otlp2parquet-core --example generate_demo_parquet -- docs/query-demo/data</code></pre>
        <p>Missing files:</p>
        <ul>${missingItems}</ul>
      `.trim();
      resultError.classList.remove('hidden');
    } else {
      updateStatus(`Initialisation failed: ${error.message}`, 'error');
    }
  }
}

async function registerSampleFiles() {
  const samples = [
    { url: 'data/logs.parquet', name: 'logs.parquet' },
    { url: 'data/metrics_gauge.parquet', name: 'metrics_gauge.parquet' },
    { url: 'data/traces.parquet', name: 'traces.parquet' }
  ];

  const failures = [];

  for (const sample of samples) {
    try {
      const response = await fetch(sample.url);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const buffer = await response.arrayBuffer();
      await db.registerFileBuffer(sample.name, new Uint8Array(buffer));
    } catch (error) {
      failures.push({ sample, error });
    }
  }

  if (failures.length > 0) {
    throw new SampleFilesMissingError(failures);
  }
}

async function runQuery() {
  const sql = sqlInput.value.trim();
  if (!sql) {
    return;
  }

  resultError.classList.add('hidden');
  resultMeta.textContent = '';
  tableWrapper.classList.add('hidden');
  resultTableHead.innerHTML = '';
  resultTableBody.innerHTML = '';
  updateStatus('Executing query…');

  try {
    const result = await conn.query(sql);
    const rows = result.toArray();
    const columns = result.schema?.fields?.map((field) => field.name) ?? (rows[0] ? Object.keys(rows[0]) : []);

    renderTable(columns, rows);

    const rowCount = rows.length;
    resultMeta.textContent = rowCount === 1 ? '1 row returned.' : `${rowCount.toLocaleString()} rows returned.`;
    tableWrapper.classList.remove('hidden');
    updateStatus('Query finished.', 'success');
  } catch (error) {
    console.error('Query failed', error);
    resultError.textContent = error.message;
    resultError.classList.remove('hidden');
    updateStatus('Query failed. See details below.', 'error');
  }
}

function renderTable(columns, rows) {
  if (columns.length === 0) {
    resultTableHead.innerHTML = '';
    resultTableBody.innerHTML = '<tr><td>No columns returned.</td></tr>';
    return;
  }

  const headerRow = document.createElement('tr');
  for (const column of columns) {
    const th = document.createElement('th');
    th.textContent = column;
    headerRow.appendChild(th);
  }
  resultTableHead.innerHTML = '';
  resultTableHead.appendChild(headerRow);

  const bodyFragment = document.createDocumentFragment();
  if (rows.length === 0) {
    const emptyRow = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = columns.length;
    cell.textContent = 'Query returned zero rows.';
    emptyRow.appendChild(cell);
    bodyFragment.appendChild(emptyRow);
  } else {
    for (const row of rows) {
      const tr = document.createElement('tr');
      const values = columns.map((column) => formatValue(row[column]));
      for (const value of values) {
        const td = document.createElement('td');
        td.textContent = value;
        tr.appendChild(td);
      }
      bodyFragment.appendChild(tr);
    }
  }

  resultTableBody.innerHTML = '';
  resultTableBody.appendChild(bodyFragment);
}

function formatValue(value) {
  if (value === null || value === undefined) {
    return '∅';
  }
  if (Array.isArray(value)) {
    return JSON.stringify(value);
  }
  if (typeof value === 'object') {
    return JSON.stringify(value);
  }
  return String(value);
}

runButton.addEventListener('click', runQuery);

sqlInput.addEventListener('keydown', (event) => {
  if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
    runQuery();
  }
});

initDuckDB();
