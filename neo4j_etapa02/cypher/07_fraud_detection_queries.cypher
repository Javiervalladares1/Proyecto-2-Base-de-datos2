// ========================================================
// ETAPA 02 - FRAUDE BANCARIO
// 07) CONSULTAS ORIENTADAS A DETECCION DE FRAUDE
// ========================================================

// FRAUDE 1: Clientes que comparten el mismo dispositivo (>= 3 clientes)
MATCH (d:Dispositivo)<-[u:USA]-(c:Cliente)
WHERE u.ultimo_uso >= datetime() - duration('P30D')
WITH d, collect(DISTINCT c.cliente_id) AS clientes, count(DISTINCT c) AS total_clientes
WHERE total_clientes >= 3
RETURN d.dispositivo_id, d.fingerprint, total_clientes, clientes
ORDER BY total_clientes DESC;

// FRAUDE 2: Cuentas con muchas alertas en 30 dias
MATCH (cu:Cuenta)<-[:DEBITA]-(t:Transaccion)-[:GENERA]->(a:Alerta)
WHERE a.fecha_creacion >= datetime() - duration('P30D')
WITH cu, count(a) AS alertas_30d, avg(a.score_alerta) AS score_promedio
WHERE alertas_30d >= 5
RETURN cu.cuenta_id, alertas_30d, score_promedio
ORDER BY alertas_30d DESC;

// FRAUDE 3: Transacciones internacionales de alto monto en horario inusual
MATCH (c:Cliente)-[:REALIZA]->(t:Transaccion)-[:DEBITA]->(cu:Cuenta)
MATCH (t)-[:SE_ORIGINA_EN]->(u:Ubicacion)
WHERE t.internacional = true
  AND t.monto >= 5000
  AND (t.fecha_hora.hour < 5 OR t.fecha_hora.hour > 22)
RETURN t.tx_id, t.fecha_hora, t.monto, c.cliente_id, cu.cuenta_id, u.pais, u.ciudad
ORDER BY t.monto DESC;

// FRAUDE 4: Ida y vuelta rapido entre cuentas vinculadas
MATCH (cu1:Cuenta)-[v:VINCULADO_A {activo:true}]->(cu2:Cuenta)
MATCH (t1:Transaccion)-[:DEBITA]->(cu1)
MATCH (t1)-[:ACREDITA]->(cu2)
MATCH (t2:Transaccion)-[:DEBITA]->(cu2)
MATCH (t2)-[:ACREDITA]->(cu1)
WHERE t1.fecha_hora >= datetime() - duration('P7D')
  AND t2.fecha_hora >= t1.fecha_hora
  AND (t2.fecha_hora.epochMillis - t1.fecha_hora.epochMillis) <= 21600000
  AND t1.monto >= 1000
  AND abs(t1.monto - t2.monto) <= 50
RETURN cu1.cuenta_id AS cuenta_a, cu2.cuenta_id AS cuenta_b, t1.tx_id AS tx_ida, t2.tx_id AS tx_vuelta, t1.monto, t2.monto;

// FRAUDE 5: Dispositivo usado por multiples clientes y multiples paises en 24h
MATCH (c:Cliente)-[:USA]->(d:Dispositivo)
MATCH (c)-[:REALIZA]->(t:Transaccion)-[:SE_ORIGINA_EN]->(u:Ubicacion)
WHERE t.fecha_hora >= datetime() - duration('P1D')
WITH d, collect(DISTINCT c.cliente_id) AS clientes, collect(DISTINCT u.pais) AS paises
WHERE size(clientes) >= 2 AND size(paises) >= 2
RETURN d.dispositivo_id, clientes, paises, size(clientes) AS n_clientes, size(paises) AS n_paises
ORDER BY n_clientes DESC, n_paises DESC;

// FRAUDE 6: Cliente operando en ciudad inusual respecto a su historico
MATCH (c:Cliente)-[:REALIZA]->(th:Transaccion)-[:SE_ORIGINA_EN]->(uh:Ubicacion)
WHERE th.fecha_hora >= datetime() - duration('P180D')
WITH c, uh.ciudad AS ciudad, count(*) AS freq
ORDER BY c.cliente_id, freq DESC
WITH c, collect(ciudad)[0] AS ciudad_habitual
MATCH (c)-[:REALIZA]->(t:Transaccion)-[:SE_ORIGINA_EN]->(u:Ubicacion)
WHERE t.fecha_hora >= datetime() - duration('P7D')
  AND u.ciudad <> ciudad_habitual
  AND t.monto >= 800
RETURN c.cliente_id, ciudad_habitual, t.tx_id, u.ciudad AS ciudad_actual, t.monto, t.fecha_hora
ORDER BY t.fecha_hora DESC;

// FRAUDE 7: Secuencias sospechosas por velocidad (OCURRE_ANTES_DE)
MATCH (t1:Transaccion)-[r:OCURRE_ANTES_DE]->(t2:Transaccion)
MATCH (t1)-[:DEBITA]->(cu:Cuenta)
WHERE r.delta_segundos <= 180
  AND r.score_patron >= 0.80
  AND t1.monto >= 500
  AND t2.monto >= 500
RETURN cu.cuenta_id, t1.tx_id, t2.tx_id, r.delta_segundos, r.patron, r.score_patron
ORDER BY r.score_patron DESC, r.delta_segundos ASC;

// FRAUDE 8: Comercios con riesgo alto por ratio de alertas
MATCH (m:Comercio)<-[:SE_DESTINA_A]-(t:Transaccion)
OPTIONAL MATCH (t)-[:GENERA]->(a:Alerta)
WITH m, count(DISTINCT t) AS total_tx, count(DISTINCT a) AS total_alertas, avg(t.monto) AS monto_promedio
WITH m, total_tx, total_alertas, monto_promedio,
     CASE WHEN total_tx = 0 THEN 0.0 ELSE toFloat(total_alertas) / toFloat(total_tx) END AS ratio_alerta
WHERE total_tx >= 30
  AND (ratio_alerta >= 0.15 OR m.blacklist = true OR m.riesgo_comercio >= 80)
RETURN m.comercio_id, m.nombre_comercio, total_tx, total_alertas, ratio_alerta, monto_promedio
ORDER BY ratio_alerta DESC, total_alertas DESC;
