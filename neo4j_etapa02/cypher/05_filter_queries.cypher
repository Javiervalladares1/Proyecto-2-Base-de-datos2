// ========================================================
// ETAPA 02 - FRAUDE BANCARIO
// 05) CONSULTAS CON FILTROS
// ========================================================

// 1) Transacciones > 3000 en ultimos 7 dias y score alto
MATCH (t:Transaccion)
WHERE t.fecha_hora >= datetime() - duration('P7D')
  AND t.monto > 3000
  AND t.score_fraude >= 0.70
RETURN t.tx_id, t.fecha_hora, t.monto, t.score_fraude
ORDER BY t.score_fraude DESC, t.monto DESC;

// 2) Clientes PEP de Guatemala con riesgo > 80
MATCH (c:Cliente:PEP)
WHERE c.pais_residencia = 'GT' AND c.riesgo_base > 80
RETURN c.cliente_id, c.nombre_completo, c.riesgo_base
ORDER BY c.riesgo_base DESC;

// 3) Dispositivos comprometidos activos en ultimos 30 dias
MATCH (d:Dispositivo:DispositivoComprometido)
WHERE d.ultima_actividad >= datetime() - duration('P30D')
RETURN d.dispositivo_id, d.ip_publica, d.rooteado, d.emulador, d.ultima_actividad;

// 4) Alertas abiertas severidad 4-5
MATCH (a:Alerta)
WHERE a.estado = 'ABIERTA' AND a.severidad >= 4
RETURN a.alerta_id, a.tipo_alerta, a.severidad, a.score_alerta, a.fecha_creacion
ORDER BY a.score_alerta DESC;

// 5) Cuentas activas con canal WEB habilitado y score_riesgo > 60
MATCH (cu:Cuenta)
WHERE cu.estado = 'ACTIVA'
  AND 'WEB' IN cu.canales_habilitados
  AND cu.score_riesgo > 60
RETURN cu.cuenta_id, cu.score_riesgo, cu.canales_habilitados
ORDER BY cu.score_riesgo DESC;
