// ========================================================
// ETAPA 02 - FRAUDE BANCARIO
// 06) CONSULTAS AGREGADAS
// ========================================================

// 1) Volumen diario de transacciones
MATCH (t:Transaccion)
WITH date(t.fecha_hora) AS dia, count(*) AS total_tx, sum(t.monto) AS monto_total
RETURN dia, total_tx, monto_total
ORDER BY dia DESC;

// 2) Monto promedio por canal
MATCH (t:Transaccion)
RETURN t.canal AS canal, count(*) AS total_tx, avg(t.monto) AS monto_promedio
ORDER BY monto_promedio DESC;

// 3) Alertas por tipo y severidad promedio
MATCH (a:Alerta)
RETURN a.tipo_alerta AS tipo, count(*) AS total_alertas, avg(a.severidad) AS severidad_promedio
ORDER BY total_alertas DESC;

// 4) Top 10 clientes por monto debitado
MATCH (c:Cliente)-[:REALIZA]->(t:Transaccion)-[:DEBITA]->(:Cuenta)
RETURN c.cliente_id, c.nombre_completo, count(t) AS tx, sum(t.monto) AS monto_total
ORDER BY monto_total DESC
LIMIT 10;

// 5) Comercios con mayor ratio de alertas
MATCH (m:Comercio)<-[:SE_DESTINA_A]-(t:Transaccion)
OPTIONAL MATCH (t)-[:GENERA]->(a:Alerta)
WITH m, count(DISTINCT t) AS total_tx, count(DISTINCT a) AS total_alertas
WITH m, total_tx, total_alertas,
     CASE WHEN total_tx = 0 THEN 0.0 ELSE toFloat(total_alertas) / toFloat(total_tx) END AS ratio_alertas
RETURN m.comercio_id, m.nombre_comercio, total_tx, total_alertas, ratio_alertas
ORDER BY ratio_alertas DESC, total_tx DESC
LIMIT 20;
