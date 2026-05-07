// ========================================================
// EXTRA - ALGORITMO DATA SCIENCE PARA DETECCION DE FRAUDE
// Opcion segura: scoring de riesgo integrado en Cypher.
// Ejecutable aunque Graph Data Science no este instalado.
// ========================================================

// 1) Calcular score de fraude por cliente
MATCH (c:Cliente)
OPTIONAL MATCH (c)-[:REALIZA]->(t:Transaccion)
OPTIONAL MATCH (t)-[:GENERA]->(a:Alerta)
OPTIONAL MATCH (c)-[:USA]->(d:Dispositivo)
OPTIONAL MATCH (c)-[cd:COMPARTE_DISPOSITIVO]->(:Cliente)
WITH
  c,
  count(DISTINCT t) AS total_tx,
  count(DISTINCT a) AS total_alertas,
  coalesce(avg(t.score_fraude), 0.0) AS promedio_score_tx,
  max(CASE WHEN d.rooteado = true OR d.emulador = true THEN 1 ELSE 0 END) AS tiene_dispositivo_comprometido,
  count(DISTINCT cd) AS conexiones_dispositivo
WITH
  c,
  total_tx,
  total_alertas,
  promedio_score_tx,
  tiene_dispositivo_comprometido,
  conexiones_dispositivo,
  (
    (coalesce(c.riesgo_base, 0.0) * 0.25) +
    (promedio_score_tx * 100.0 * 0.35) +
    (CASE WHEN total_alertas >= 5 THEN 20.0 ELSE total_alertas * 4.0 END) +
    (CASE WHEN tiene_dispositivo_comprometido = 1 THEN 12.0 ELSE 0.0 END) +
    (CASE WHEN conexiones_dispositivo >= 3 THEN 10.0 ELSE conexiones_dispositivo * 3.0 END)
  ) AS score_calculado
SET c.score_ds_fraude = round(CASE WHEN score_calculado > 100 THEN 100 ELSE score_calculado END, 2),
    c.total_alertas_ds = total_alertas,
    c.total_tx_ds = total_tx,
    c.fecha_score_ds = datetime(),
    c:ClienteScoreado
FOREACH (_ IN CASE WHEN score_calculado >= 70 THEN [1] ELSE [] END | SET c:ClienteRiesgoCritico)
RETURN
  c.cliente_id,
  c.nombre_completo,
  c.riesgo_base,
  c.score_ds_fraude,
  total_alertas,
  total_tx,
  tiene_dispositivo_comprometido,
  conexiones_dispositivo
ORDER BY c.score_ds_fraude DESC
LIMIT 25;

// 2) Distribucion del score por bandas
MATCH (c:ClienteScoreado)
WITH
  CASE
    WHEN c.score_ds_fraude >= 70 THEN 'CRITICO'
    WHEN c.score_ds_fraude >= 60 THEN 'ALTO'
    WHEN c.score_ds_fraude >= 40 THEN 'MEDIO'
    ELSE 'BAJO'
  END AS banda_riesgo,
  count(c) AS clientes
RETURN banda_riesgo, clientes
ORDER BY clientes DESC;

// 3) Explicabilidad del score para un cliente critico
MATCH (c:ClienteRiesgoCritico)
OPTIONAL MATCH (c)-[:REALIZA]->(t:Transaccion)-[:GENERA]->(a:Alerta)
OPTIONAL MATCH (c)-[:USA]->(d:Dispositivo)
RETURN
  c.cliente_id,
  c.nombre_completo,
  c.score_ds_fraude,
  collect(DISTINCT a.tipo_alerta)[0..5] AS tipos_alerta,
  collect(DISTINCT d.dispositivo_id)[0..5] AS dispositivos,
  collect(DISTINCT t.tx_id)[0..5] AS transacciones_relacionadas
ORDER BY c.score_ds_fraude DESC
LIMIT 10;

// ========================================================
// OPCION GDS SI LA INSTANCIA TIENE GRAPH DATA SCIENCE.
// Ejecutar solo si `RETURN gds.version()` funciona.
// ========================================================

// RETURN gds.version() AS version_gds;

// CALL gds.graph.drop('fraude_clientes', false) YIELD graphName;

// CALL gds.graph.project(
//   'fraude_clientes',
//   ['Cliente'],
//   {
//     COMPARTE_DISPOSITIVO: {orientation: 'UNDIRECTED'}
//   }
// )
// YIELD graphName, nodeCount, relationshipCount;

// CALL gds.pageRank.stream('fraude_clientes')
// YIELD nodeId, score
// MATCH (c:Cliente) WHERE id(c) = nodeId
// SET c.score_pagerank_red = score
// RETURN c.cliente_id, c.nombre_completo, score
// ORDER BY score DESC
// LIMIT 20;
