const state = {
  metrics: null,
  currentView: 'dashboard',
};

const fraudOptions = [
  ['shared-devices', 'Dispositivos compartidos'],
  ['alert-accounts', 'Cuentas con alertas'],
  ['international-high', 'Internacionales alto monto'],
  ['unusual-location', 'Ubicacion inusual'],
  ['sequences', 'Secuencias rapidas'],
  ['merchants', 'Comercios riesgosos'],
];

const rubricItems = [
  ['5+ labels', 'Cliente, Cuenta, Transaccion, Dispositivo, Ubicacion, Comercio y Alerta.'],
  ['5+ propiedades', 'Cada label muestra propiedades de negocio y tipos de datos.'],
  ['10+ relaciones', 'El modelo usa 13 tipos dirigidos con propiedades.'],
  ['Tipos de datos', 'String, Float, Integer, Boolean, List, Date y DateTime.'],
  ['5000+ nodos', 'El dataset cargado supera ampliamente el minimo solicitado.'],
  ['Grafo conexo', 'La app verifica nodos aislados y relaciones backbone.'],
  ['Carga CSV', 'La pantalla CSV permite crear nodos desde archivo.'],
  ['CRUD', 'La pantalla CRUD crea nodos, labels multiples, relaciones y actualiza propiedades.'],
  ['Consultas Cypher', 'La seccion Fraude ejecuta consultas listas para exponer.'],
  ['Extra DS', 'El boton de scoring calcula riesgo de fraude por cliente.'],
];

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

async function api(path, options = {}) {
  const response = await fetch(path, options);
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || 'Error de servidor');
  return data;
}

function toast(message) {
  const node = $('#toast');
  node.textContent = message;
  node.classList.add('visible');
  setTimeout(() => node.classList.remove('visible'), 2600);
}

function formatValue(value) {
  if (Array.isArray(value)) return value.join(', ');
  if (value && typeof value === 'object') return JSON.stringify(value);
  return String(value ?? '');
}

function renderTable(container, rows) {
  if (!rows?.length) {
    container.innerHTML = '<p class="empty">Sin resultados para esta consulta.</p>';
    return;
  }

  const headers = Object.keys(rows[0]);
  container.innerHTML = `
    <table>
      <thead><tr>${headers.map((header) => `<th>${header}</th>`).join('')}</tr></thead>
      <tbody>
        ${rows
          .map(
            (row) =>
              `<tr>${headers.map((header) => `<td>${formatValue(row[header])}</td>`).join('')}</tr>`,
          )
          .join('')}
      </tbody>
    </table>
  `;
}

function renderMetrics(metrics) {
  const cards = [
    ['Nodos', metrics.totals.total_nodos],
    ['Relaciones', metrics.totals.total_relaciones],
    ['Alertas criticas', metrics.risk.alertas_criticas],
    ['Clientes criticos', metrics.risk.clientes_riesgo_critico || 0],
    ['Nodos aislados', metrics.risk.aislados],
  ];

  $('#metricGrid').innerHTML = cards
    .map(
      ([label, value]) => `
        <article class="metric">
          <span>${label}</span>
          <strong>${Number(value).toLocaleString('es-GT')}</strong>
        </article>
      `,
    )
    .join('');

  renderBars($('#labelBars'), metrics.labels, 'label');
  renderBars($('#relationshipBars'), metrics.relationships, 'tipo');
}

function renderBars(container, rows, key) {
  const max = Math.max(...rows.map((row) => row.total), 1);
  container.innerHTML = rows
    .map((row) => {
      const pct = Math.max((row.total / max) * 100, 3);
      return `
        <div class="bar-row">
          <strong>${row[key]}</strong>
          <div class="bar-track"><div class="bar-fill" style="width:${pct}%"></div></div>
          <span>${Number(row.total).toLocaleString('es-GT')}</span>
        </div>
      `;
    })
    .join('');
}

async function loadHealth() {
  try {
    const health = await api('/api/health');
    $('#healthDot').className = 'dot ok';
    $('#healthText').textContent = 'Conectado';
    $('#healthDb').textContent = health.database;
  } catch (error) {
    $('#healthDot').className = 'dot bad';
    $('#healthText').textContent = 'Sin conexion';
    $('#healthDb').textContent = error.message;
  }
}

async function loadMetrics() {
  state.metrics = await api('/api/metrics');
  renderMetrics(state.metrics);
}

function switchView(view) {
  state.currentView = view;
  $$('.view').forEach((node) => node.classList.toggle('active', node.id === view));
  $$('.nav-button').forEach((button) => button.classList.toggle('active', button.dataset.view === view));
}

function renderGraph(graph) {
  const svg = $('#graphSvg');
  const width = svg.clientWidth || 900;
  const height = svg.clientHeight || 620;
  const cx = width / 2;
  const cy = height / 2;
  const radius = Math.min(width, height) * 0.38;
  const colors = {
    Cliente: '#1769aa',
    Cuenta: '#2f855a',
    Transaccion: '#b7791f',
    Dispositivo: '#805ad5',
    Ubicacion: '#319795',
    Comercio: '#dd6b20',
    Alerta: '#c53030',
  };

  const nodes = graph.nodes || [];
  const rels = graph.relationships || [];
  const positions = new Map();

  nodes.forEach((node, index) => {
    const angle = (Math.PI * 2 * index) / Math.max(nodes.length, 1) - Math.PI / 2;
    const label = node.labels[0];
    const bias = label === 'Cliente' ? 0.18 : label === 'Transaccion' ? 0.68 : 1;
    positions.set(node.id, {
      x: cx + Math.cos(angle) * radius * bias,
      y: cy + Math.sin(angle) * radius * bias,
    });
  });

  svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
  svg.innerHTML = `
    <defs>
      <marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
        <path d="M 0 0 L 10 5 L 0 10 z" fill="#29313a"></path>
      </marker>
    </defs>
    ${rels
      .map((rel) => {
        const source = positions.get(rel.source);
        const target = positions.get(rel.target);
        if (!source || !target) return '';
        const midX = (source.x + target.x) / 2;
        const midY = (source.y + target.y) / 2;
        return `
          <line x1="${source.x}" y1="${source.y}" x2="${target.x}" y2="${target.y}" stroke="#29313a" stroke-width="1.6" marker-end="url(#arrow)" opacity="0.7"></line>
          <text x="${midX}" y="${midY}" fill="#29313a" font-size="10" text-anchor="middle" paint-order="stroke" stroke="#fff" stroke-width="4">${rel.type}</text>
        `;
      })
      .join('')}
    ${nodes
      .map((node) => {
        const pos = positions.get(node.id);
        const label = node.labels[0];
        const color = colors[label] || '#475569';
        return `
          <g transform="translate(${pos.x} ${pos.y})">
            <circle r="28" fill="#fff" stroke="${color}" stroke-width="4"></circle>
            <text y="-4" text-anchor="middle" font-size="10" font-weight="700" fill="${color}">${label}</text>
            <text y="12" text-anchor="middle" font-size="9" fill="#17202a">${node.caption}</text>
          </g>
        `;
      })
      .join('')}
  `;

  const labels = [...new Set(nodes.map((node) => node.labels[0]))];
  $('#graphLegend').innerHTML = labels
    .map((label) => `<span style="border-left:4px solid ${colors[label] || '#475569'}">${label}</span>`)
    .join('');
}

async function loadGraph(clienteId) {
  const params = clienteId ? `?clienteId=${encodeURIComponent(clienteId)}` : '';
  const graph = await api(`/api/graph${params}`);
  renderGraph(graph);
  toast(`Grafo actualizado: ${graph.nodes.length} nodos`);
}

function setupFraudButtons() {
  $('#fraudButtons').innerHTML = [
    ...fraudOptions.map(([kind, label]) => `<button data-fraud="${kind}">${label}</button>`),
    '<button data-ds="score" class="primary">Ejecutar scoring DS</button>',
  ].join('');

  $$('[data-fraud]').forEach((button) => {
    button.addEventListener('click', async () => {
      $('#fraudTable').innerHTML = '<p>Ejecutando consulta...</p>';
      const data = await api(`/api/fraud/${button.dataset.fraud}`);
      renderTable($('#fraudTable'), data.rows);
    });
  });

  $('[data-ds="score"]').addEventListener('click', async () => {
    $('#fraudTable').innerHTML = '<p>Calculando score de fraude...</p>';
    const data = await api('/api/ds/score', { method: 'POST' });
    renderTable($('#fraudTable'), data.rows);
    toast('Score DS actualizado en Neo4j');
    await loadMetrics();
  });
}

function setupCrud() {
  const output = $('#crudOutput');
  $$('[data-demo]').forEach((button) => {
    button.addEventListener('click', async () => {
      const action = button.dataset.demo;
      const method = action === 'update' ? 'PATCH' : action === 'cleanup' ? 'DELETE' : 'POST';
      output.textContent = 'Ejecutando...';
      const data = await api(`/api/demo/${action}`, { method });
      output.textContent = JSON.stringify(data.rows, null, 2);
      toast('Operacion ejecutada');
      await loadMetrics();
    });
  });
}

function setupCsv() {
  $('#downloadSampleBtn').addEventListener('click', () => {
    const sample = [
      'cliente_id,nombre_completo,documento_id,fecha_nacimiento,fecha_alta,segmento,ingresos_mensuales,pep,pais_residencia,riesgo_base,telefonos,emails,productos_activos',
      'CCSV001,Cliente CSV Demo,DPI-CSV-001,1991-02-10,2026-05-06,PERSONAL,2100.00,false,GT,33.5,+50255551111,csv.demo@correo.com,AHORRO|TARJETA_DEBITO',
    ].join('\n');
    const blob = new Blob([sample], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'clientes_demo.csv';
    link.click();
    URL.revokeObjectURL(url);
  });

  $('#csvForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const file = $('#csvFile').files[0];
    if (!file) {
      toast('Selecciona un CSV primero');
      return;
    }
    const formData = new FormData();
    formData.append('file', file);
    $('#csvOutput').textContent = 'Cargando archivo...';
    const data = await api('/api/import/clientes', { method: 'POST', body: formData });
    $('#csvOutput').textContent = JSON.stringify(data, null, 2);
    toast('CSV cargado');
    await loadMetrics();
  });
}

function setupRubric() {
  $('#rubricList').innerHTML = rubricItems
    .map(
      ([title, detail]) => `
        <div class="check-item">
          <strong>${title}</strong>
          <span>${detail}</span>
        </div>
      `,
    )
    .join('');
}

function setupNavigation() {
  $$('.nav-button').forEach((button) => {
    button.addEventListener('click', () => switchView(button.dataset.view));
  });
  $('#refreshBtn').addEventListener('click', async () => {
    await loadHealth();
    await loadMetrics();
    toast('Datos actualizados');
  });
  $('#loadGraphBtn').addEventListener('click', () => loadGraph($('#clienteIdInput').value.trim()));
  $('#loadSampleGraphBtn').addEventListener('click', () => loadGraph());
}

async function boot() {
  setupNavigation();
  setupFraudButtons();
  setupCrud();
  setupCsv();
  setupRubric();

  await loadHealth();
  await loadMetrics();
  await loadGraph();
}

boot().catch((error) => {
  toast(error.message);
  console.error(error);
});
