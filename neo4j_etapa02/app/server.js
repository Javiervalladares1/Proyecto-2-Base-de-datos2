import 'dotenv/config';
import express from 'express';
import multer from 'multer';
import neo4j from 'neo4j-driver';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 2 * 1024 * 1024 } });
const port = Number(process.env.PORT || 5174);
const database = process.env.NEO4J_DATABASE || 'neo4j';

app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public')));

const driver = neo4j.driver(
  process.env.NEO4J_URI || 'neo4j://localhost:7687',
  neo4j.auth.basic(process.env.NEO4J_USERNAME || 'neo4j', process.env.NEO4J_PASSWORD || 'password'),
);

function toNative(value) {
  if (neo4j.isInt(value)) return value.toNumber();
  if (Array.isArray(value)) return value.map(toNative);
  if (value && typeof value === 'object') {
    if (value.year && value.month && value.day && typeof value.toString === 'function') return value.toString();
    if (value.hour !== undefined && value.minute !== undefined && typeof value.toString === 'function') return value.toString();
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, toNative(item)]));
  }
  return value;
}

function nodeToGraph(node) {
  const props = toNative(node.properties);
  const id =
    props.cliente_id ||
    props.cuenta_id ||
    props.tx_id ||
    props.dispositivo_id ||
    props.ubicacion_id ||
    props.comercio_id ||
    props.alerta_id ||
    node.identity.toString();

  return {
    id: node.identity.toString(),
    businessId: id,
    labels: node.labels,
    caption: id,
    properties: props,
  };
}

function relToGraph(rel) {
  return {
    id: rel.identity.toString(),
    source: rel.start.toString(),
    target: rel.end.toString(),
    type: rel.type,
    properties: toNative(rel.properties),
  };
}

function rowsFromResult(result) {
  return result.records.map((record) =>
    Object.fromEntries(record.keys.map((key) => [key, toNative(record.get(key))])),
  );
}

async function runRead(query, params = {}) {
  const session = driver.session({ database });
  try {
    const result = await session.executeRead((tx) => tx.run(query, params));
    return rowsFromResult(result);
  } finally {
    await session.close();
  }
}

async function runWrite(query, params = {}) {
  const session = driver.session({ database });
  try {
    const result = await session.executeWrite((tx) => tx.run(query, params));
    return rowsFromResult(result);
  } finally {
    await session.close();
  }
}

function parseCsv(text) {
  const rows = [];
  let current = '';
  let row = [];
  let inQuotes = false;

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];
    const next = text[i + 1];

    if (char === '"' && inQuotes && next === '"') {
      current += '"';
      i += 1;
    } else if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      row.push(current.trim());
      current = '';
    } else if ((char === '\n' || char === '\r') && !inQuotes) {
      if (char === '\r' && next === '\n') i += 1;
      row.push(current.trim());
      if (row.some(Boolean)) rows.push(row);
      row = [];
      current = '';
    } else {
      current += char;
    }
  }

  if (current || row.length) {
    row.push(current.trim());
    if (row.some(Boolean)) rows.push(row);
  }

  const [headers, ...data] = rows;
  if (!headers?.length) return [];
  return data.map((items) =>
    Object.fromEntries(headers.map((header, index) => [header, items[index] ?? ''])),
  );
}

function graphFromRecords(records) {
  const nodeMap = new Map();
  const relMap = new Map();

  for (const record of records) {
    for (const value of Object.values(record)) {
      if (!value) continue;
      if (value.labels && value.properties) nodeMap.set(value.identity.toString(), nodeToGraph(value));
      if (value.type && value.start !== undefined && value.end !== undefined) {
        relMap.set(value.identity.toString(), relToGraph(value));
      }
    }
  }

  return { nodes: [...nodeMap.values()], relationships: [...relMap.values()] };
}

app.get('/api/health', async (_req, res) => {
  try {
    const [row] = await runRead('RETURN 1 AS ok');
    res.json({ ok: row.ok === 1, database });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/metrics', async (_req, res) => {
  try {
    const [totals] = await runRead(`
      MATCH (n)
      WITH count(n) AS total_nodos
      MATCH ()-[r]->()
      RETURN total_nodos, count(r) AS total_relaciones
    `);
    const labels = await runRead(`
      MATCH (n)
      UNWIND labels(n) AS label
      RETURN label, count(*) AS total
      ORDER BY total DESC
    `);
    const relationships = await runRead(`
      MATCH ()-[r]->()
      RETURN type(r) AS tipo, count(r) AS total
      ORDER BY total DESC
    `);
    const [risk] = await runRead(`
      MATCH (n)
      WHERE NOT (n)--()
      WITH count(n) AS aislados
      MATCH (a:AlertaCritica)
      WITH aislados, count(a) AS alertas_criticas
      MATCH (t:TransaccionSospechosa)
      WITH aislados, alertas_criticas, count(t) AS transacciones_sospechosas
      MATCH (c:ClienteRiesgoAlto)
      WITH aislados, alertas_criticas, transacciones_sospechosas, count(c) AS clientes_riesgo_alto
      MATCH (cc:ClienteRiesgoCritico)
      RETURN aislados, alertas_criticas, transacciones_sospechosas, clientes_riesgo_alto, count(cc) AS clientes_riesgo_critico
    `);
    res.json({ totals, labels, relationships, risk });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/graph', async (req, res) => {
  const session = driver.session({ database });
  try {
    const clienteId = String(req.query.clienteId || '').trim();
    const result = clienteId
      ? await session.executeRead((tx) =>
          tx.run(
            `
            MATCH (c:Cliente {cliente_id: $clienteId})
            MATCH p=(c)-[*1..2]-(n)
            UNWIND relationships(p) AS rel
            WITH collect(DISTINCT c) + collect(DISTINCT n) AS nodes, collect(DISTINCT rel) AS rels
            UNWIND nodes AS node
            WITH collect(DISTINCT node) AS nodeList, rels
            UNWIND rels AS rel
            RETURN nodeList, collect(DISTINCT rel) AS relList
            LIMIT 1
            `,
            { clienteId },
          ),
        )
      : await session.executeRead((tx) =>
          tx.run(`
            MATCH (c:Cliente)-[r:REALIZA]->(t:Transaccion)
            OPTIONAL MATCH (t)-[g:GENERA]->(a:Alerta)
            OPTIONAL MATCH (t)-[o:SE_ORIGINA_EN]->(u:Ubicacion)
            RETURN c, r, t, g, a, o, u
            ORDER BY t.score_fraude DESC
            LIMIT 60
          `),
        );

    if (clienteId) {
      const record = result.records[0];
      const nodes = (record?.get('nodeList') || []).map(nodeToGraph);
      const relationships = (record?.get('relList') || []).map(relToGraph);
      res.json({ nodes, relationships });
      return;
    }

    res.json(graphFromRecords(result.records.map((record) => Object.fromEntries(record.keys.map((key) => [key, record.get(key)])))));
  } catch (error) {
    res.status(500).json({ error: error.message });
  } finally {
    await session.close();
  }
});

const fraudQueries = {
  'shared-devices': `
    MATCH (mx:Transaccion) WITH max(mx.fecha_hora) AS ref
    MATCH (d:Dispositivo)<-[u:USA]-(c:Cliente)
    WHERE u.ultimo_uso >= ref - duration('P30D')
    WITH d, collect(DISTINCT c.cliente_id) AS clientes, count(DISTINCT c) AS total_clientes
    WHERE total_clientes >= 3
    RETURN d.dispositivo_id AS dispositivo, d.fingerprint AS fingerprint, total_clientes, clientes
    ORDER BY total_clientes DESC LIMIT 25
  `,
  'alert-accounts': `
    MATCH (mx:Transaccion) WITH max(mx.fecha_hora) AS ref
    MATCH (cu:Cuenta)<-[:DEBITA]-(t:Transaccion)-[:GENERA]->(a:Alerta)
    WHERE a.fecha_creacion >= ref - duration('P30D')
    WITH cu, count(a) AS alertas_30d, round(avg(a.score_alerta), 3) AS score_promedio
    WHERE alertas_30d >= 3
    RETURN cu.cuenta_id AS cuenta, alertas_30d, score_promedio
    ORDER BY alertas_30d DESC LIMIT 25
  `,
  'international-high': `
    MATCH (c:Cliente)-[:REALIZA]->(t:Transaccion)-[:DEBITA]->(cu:Cuenta)
    MATCH (t)-[:SE_ORIGINA_EN]->(u:Ubicacion)
    WHERE t.internacional = true AND t.monto >= 5000 AND (t.fecha_hora.hour < 5 OR t.fecha_hora.hour > 22)
    RETURN t.tx_id AS transaccion, t.fecha_hora AS fecha, t.monto AS monto, c.cliente_id AS cliente,
           cu.cuenta_id AS cuenta, u.pais AS pais, u.ciudad AS ciudad, t.score_fraude AS score
    ORDER BY t.monto DESC LIMIT 25
  `,
  'unusual-location': `
    MATCH (c:Cliente)-[:REALIZA]->(th:Transaccion)-[:SE_ORIGINA_EN]->(uh:Ubicacion)
    WITH c, uh.ciudad AS ciudad, count(*) AS freq
    ORDER BY c.cliente_id, freq DESC
    WITH c, collect(ciudad)[0] AS ciudad_habitual
    MATCH (mx:Transaccion) WITH c, ciudad_habitual, max(mx.fecha_hora) AS ref
    MATCH (c)-[:REALIZA]->(t:Transaccion)-[:SE_ORIGINA_EN]->(u:Ubicacion)
    WHERE t.fecha_hora >= ref - duration('P30D') AND u.ciudad <> ciudad_habitual AND t.monto >= 800
    RETURN c.cliente_id AS cliente, ciudad_habitual, t.tx_id AS transaccion, u.ciudad AS ciudad_actual,
           t.monto AS monto, t.fecha_hora AS fecha
    ORDER BY t.fecha_hora DESC LIMIT 25
  `,
  sequences: `
    MATCH (t1:Transaccion)-[r:OCURRE_ANTES_DE]->(t2:Transaccion)
    MATCH (t1)-[:DEBITA]->(cu:Cuenta)
    WHERE r.delta_segundos <= 180 AND r.score_patron >= 0.80 AND t1.monto >= 500 AND t2.monto >= 500
    RETURN cu.cuenta_id AS cuenta, t1.tx_id AS tx_inicial, t2.tx_id AS tx_siguiente,
           r.delta_segundos AS segundos, r.patron AS patron, r.score_patron AS score
    ORDER BY r.score_patron DESC, r.delta_segundos ASC LIMIT 25
  `,
  merchants: `
    MATCH (m:Comercio)<-[:SE_DESTINA_A]-(t:Transaccion)
    OPTIONAL MATCH (t)-[:GENERA]->(a:Alerta)
    WITH m, count(DISTINCT t) AS total_tx, count(DISTINCT a) AS total_alertas, round(avg(t.monto), 2) AS monto_promedio
    WITH m, total_tx, total_alertas, monto_promedio,
         CASE WHEN total_tx = 0 THEN 0.0 ELSE round(toFloat(total_alertas) / toFloat(total_tx), 3) END AS ratio_alerta
    WHERE total_tx >= 10 AND (ratio_alerta >= 0.15 OR m.blacklist = true OR m.riesgo_comercio >= 80)
    RETURN m.comercio_id AS comercio, m.nombre_comercio AS nombre, m.riesgo_comercio AS riesgo,
           m.blacklist AS blacklist, total_tx, total_alertas, ratio_alerta, monto_promedio
    ORDER BY ratio_alerta DESC, total_alertas DESC LIMIT 25
  `,
};

app.get('/api/fraud/:kind', async (req, res) => {
  try {
    const query = fraudQueries[req.params.kind];
    if (!query) {
      res.status(404).json({ error: 'Consulta no encontrada' });
      return;
    }
    res.json({ rows: await runRead(query) });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/ds/score', async (_req, res) => {
  try {
    const rows = await runWrite(`
      MATCH (c:Cliente)
      OPTIONAL MATCH (c)-[:REALIZA]->(t:Transaccion)
      OPTIONAL MATCH (t)-[:GENERA]->(a:Alerta)
      OPTIONAL MATCH (c)-[:USA]->(d:Dispositivo)
      OPTIONAL MATCH (c)-[cd:COMPARTE_DISPOSITIVO]->(:Cliente)
      WITH c, count(DISTINCT t) AS total_tx, count(DISTINCT a) AS total_alertas,
           coalesce(avg(t.score_fraude), 0.0) AS promedio_score_tx,
           max(CASE WHEN d.rooteado = true OR d.emulador = true THEN 1 ELSE 0 END) AS dispositivo_comprometido,
           count(DISTINCT cd) AS conexiones_dispositivo
      WITH c, total_tx, total_alertas, promedio_score_tx, dispositivo_comprometido, conexiones_dispositivo,
           ((coalesce(c.riesgo_base, 0.0) * 0.25) +
            (promedio_score_tx * 100.0 * 0.35) +
            (CASE WHEN total_alertas >= 5 THEN 20.0 ELSE total_alertas * 4.0 END) +
            (CASE WHEN dispositivo_comprometido = 1 THEN 12.0 ELSE 0.0 END) +
            (CASE WHEN conexiones_dispositivo >= 3 THEN 10.0 ELSE conexiones_dispositivo * 3.0 END)) AS score_calculado
      SET c.score_ds_fraude = round(CASE WHEN score_calculado > 100 THEN 100 ELSE score_calculado END, 2),
          c.total_alertas_ds = total_alertas,
          c.total_tx_ds = total_tx,
          c.fecha_score_ds = datetime(),
          c:ClienteScoreado
      FOREACH (_ IN CASE WHEN score_calculado >= 70 THEN [1] ELSE [] END | SET c:ClienteRiesgoCritico)
      RETURN c.cliente_id AS cliente, c.nombre_completo AS nombre, c.score_ds_fraude AS score,
             total_alertas, total_tx, dispositivo_comprometido, conexiones_dispositivo
      ORDER BY score DESC LIMIT 25
    `);
    res.json({ rows });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/demo/create-one', async (_req, res) => {
  try {
    const rows = await runWrite(`
      MERGE (c:Cliente {cliente_id:'CAPP001'})
      SET c.nombre_completo = 'Cliente App Demo',
          c.documento_id = 'DPI-APP-001',
          c.fecha_nacimiento = date('1994-05-12'),
          c.fecha_alta = date(),
          c.segmento = 'PERSONAL',
          c.ingresos_mensuales = 3200.0,
          c.pep = false,
          c.pais_residencia = 'GT',
          c.riesgo_base = 31.5,
          c.telefonos = ['+50255550123'],
          c.emails = ['cliente.app.demo@correo.com'],
          c.productos_activos = ['AHORRO', 'TARJETA_DEBITO'],
          c.ultima_actualizacion = datetime()
      RETURN c.cliente_id AS cliente, labels(c) AS labels, keys(c) AS propiedades
    `);
    res.json({ rows });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/demo/create-multi', async (_req, res) => {
  try {
    const rows = await runWrite(`
      MERGE (c:Cliente:PEP:ClienteRiesgoAlto {cliente_id:'CAPP002'})
      SET c.nombre_completo = 'Cliente App Riesgo',
          c.documento_id = 'DPI-APP-002',
          c.fecha_nacimiento = date('1979-10-03'),
          c.fecha_alta = date(),
          c.segmento = 'PREMIUM',
          c.ingresos_mensuales = 18000.0,
          c.pep = true,
          c.pais_residencia = 'GT',
          c.riesgo_base = 92.0,
          c.telefonos = ['+50255550456'],
          c.emails = ['cliente.riesgo.app@correo.com'],
          c.productos_activos = ['AHORRO', 'INVERSION', 'CORRIENTE'],
          c.ultima_actualizacion = datetime()
      RETURN c.cliente_id AS cliente, labels(c) AS labels, keys(c) AS propiedades
    `);
    res.json({ rows });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/demo/create-relation', async (_req, res) => {
  try {
    const rows = await runWrite(`
      MATCH (c:Cliente {cliente_id:'CAPP001'})
      MATCH (cu:Cuenta {cuenta_id:'CU000001'})
      MERGE (c)-[r:POSEE]->(cu)
      SET r.fecha_inicio = date(),
          r.tipo_titularidad = 'PRINCIPAL',
          r.porcentaje_propiedad = 100.0,
          r.estado_relacion = 'ACTIVA',
          r.origen_onboarding = 'APP_FRONTEND'
      RETURN c.cliente_id AS cliente, type(r) AS relacion, cu.cuenta_id AS cuenta, keys(r) AS propiedades
    `);
    res.json({ rows });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.patch('/api/demo/update', async (_req, res) => {
  try {
    const rows = await runWrite(`
      MATCH (c:Cliente)
      WHERE c.cliente_id IN ['CAPP001', 'CAPP002']
      SET c.revision_frontend = true,
          c.fecha_revision_frontend = date(),
          c.ultima_actualizacion = datetime()
      RETURN c.cliente_id AS cliente, c.revision_frontend AS revision, c.fecha_revision_frontend AS fecha
      ORDER BY cliente
    `);
    res.json({ rows });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.delete('/api/demo/cleanup', async (_req, res) => {
  try {
    const rows = await runWrite(`
      MATCH (c:Cliente)
      WHERE c.cliente_id IN ['CAPP001', 'CAPP002']
      WITH collect(c) AS nodos, count(c) AS total
      FOREACH (n IN nodos | DETACH DELETE n)
      RETURN total AS nodos_eliminados
    `);
    res.json({ rows });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/import/clientes', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      res.status(400).json({ error: 'Sube un archivo CSV en el campo file.' });
      return;
    }
    const rows = parseCsv(req.file.buffer.toString('utf8')).slice(0, 500);
    const loaded = await runWrite(
      `
      UNWIND $rows AS row
      MERGE (c:Cliente {cliente_id: row.cliente_id})
      SET c.nombre_completo = row.nombre_completo,
          c.documento_id = row.documento_id,
          c.fecha_nacimiento = date(row.fecha_nacimiento),
          c.fecha_alta = date(row.fecha_alta),
          c.segmento = row.segmento,
          c.ingresos_mensuales = toFloat(row.ingresos_mensuales),
          c.pep = coalesce(toBoolean(row.pep), false),
          c.pais_residencia = row.pais_residencia,
          c.riesgo_base = toFloat(row.riesgo_base),
          c.telefonos = CASE WHEN row.telefonos IS NULL OR trim(row.telefonos) = '' THEN [] ELSE split(row.telefonos, '|') END,
          c.emails = CASE WHEN row.emails IS NULL OR trim(row.emails) = '' THEN [] ELSE split(row.emails, '|') END,
          c.productos_activos = CASE WHEN row.productos_activos IS NULL OR trim(row.productos_activos) = '' THEN [] ELSE split(row.productos_activos, '|') END,
          c.ultima_actualizacion = datetime()
      RETURN count(c) AS clientes_cargados
      `,
      { rows },
    );
    res.json({ rows: loaded, preview: rows.slice(0, 3) });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.listen(port, () => {
  console.log(`Fraude Bancario Neo4j app en http://localhost:${port}`);
});

process.on('SIGINT', async () => {
  await driver.close();
  process.exit(0);
});
