// ========================================================
// ETAPA 02 - FRAUDE BANCARIO
// 03) CARGA DE RELACIONES DESDE CSV
// ========================================================

// ---------- Cliente POSEE Cuenta ----------
LOAD CSV WITH HEADERS FROM 'file:///rel_cliente_posee_cuenta.csv' AS row
CALL {
  WITH row
  MATCH (c:Cliente {cliente_id: row.cliente_id})
  MATCH (cu:Cuenta {cuenta_id: row.cuenta_id})
  MERGE (c)-[r:POSEE]->(cu)
  SET r.fecha_inicio = date(row.fecha_inicio),
      r.tipo_titularidad = row.tipo_titularidad,
      r.porcentaje_propiedad = toFloat(row.porcentaje_propiedad),
      r.estado_relacion = row.estado_relacion,
      r.origen_onboarding = row.origen_onboarding
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Cliente REALIZA Transaccion ----------
LOAD CSV WITH HEADERS FROM 'file:///rel_cliente_realiza_tx.csv' AS row
CALL {
  WITH row
  MATCH (c:Cliente {cliente_id: row.cliente_id})
  MATCH (t:Transaccion {tx_id: row.tx_id})
  MERGE (c)-[r:REALIZA]->(t)
  SET r.rol = row.rol,
      r.factor_autenticacion = row.factor_autenticacion,
      r.autenticacion_ok = coalesce(toBoolean(row.autenticacion_ok), false),
      r.ip_origen = row.ip_origen,
      r.confianza_sesion = toFloat(row.confianza_sesion)
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Transaccion DEBITA Cuenta ----------
LOAD CSV WITH HEADERS FROM 'file:///rel_tx_debita_cuenta.csv' AS row
CALL {
  WITH row
  MATCH (t:Transaccion {tx_id: row.tx_id})
  MATCH (cu:Cuenta {cuenta_id: row.cuenta_id})
  MERGE (t)-[r:DEBITA]->(cu)
  SET r.monto = toFloat(row.monto),
      r.moneda = row.moneda,
      r.saldo_previo = toFloat(row.saldo_previo),
      r.saldo_posterior = toFloat(row.saldo_posterior),
      r.timestamp_contable = datetime(row.timestamp_contable)
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Transaccion ACREDITA Cuenta ----------
LOAD CSV WITH HEADERS FROM 'file:///rel_tx_acredita_cuenta.csv' AS row
CALL {
  WITH row
  MATCH (t:Transaccion {tx_id: row.tx_id})
  MATCH (cu:Cuenta {cuenta_id: row.cuenta_id})
  MERGE (t)-[r:ACREDITA]->(cu)
  SET r.monto = toFloat(row.monto),
      r.moneda = row.moneda,
      r.saldo_previo = toFloat(row.saldo_previo),
      r.saldo_posterior = toFloat(row.saldo_posterior),
      r.timestamp_contable = datetime(row.timestamp_contable)
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Cliente USA Dispositivo ----------
LOAD CSV WITH HEADERS FROM 'file:///rel_cliente_usa_dispositivo.csv' AS row
CALL {
  WITH row
  MATCH (c:Cliente {cliente_id: row.cliente_id})
  MATCH (d:Dispositivo {dispositivo_id: row.dispositivo_id})
  MERGE (c)-[r:USA]->(d)
  SET r.primer_uso = date(row.primer_uso),
      r.ultimo_uso = datetime(row.ultimo_uso),
      r.veces_uso = toInteger(row.veces_uso),
      r.confianza_dispositivo = toFloat(row.confianza_dispositivo),
      r.activo = coalesce(toBoolean(row.activo), true)
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Transaccion SE_ORIGINA_EN Ubicacion ----------
LOAD CSV WITH HEADERS FROM 'file:///rel_tx_origina_ubicacion.csv' AS row
CALL {
  WITH row
  MATCH (t:Transaccion {tx_id: row.tx_id})
  MATCH (u:Ubicacion {ubicacion_id: row.ubicacion_id})
  MERGE (t)-[r:SE_ORIGINA_EN]->(u)
  SET r.fecha_hora = datetime(row.fecha_hora),
      r.precision_metros = toInteger(row.precision_metros),
      r.metodo_geolocalizacion = row.metodo_geolocalizacion,
      r.riesgo_geo = toInteger(row.riesgo_geo),
      r.es_vpn = coalesce(toBoolean(row.es_vpn), false)
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Transaccion SE_DESTINA_A Comercio ----------
LOAD CSV WITH HEADERS FROM 'file:///rel_tx_destina_comercio.csv' AS row
CALL {
  WITH row
  MATCH (t:Transaccion {tx_id: row.tx_id})
  MATCH (m:Comercio {comercio_id: row.comercio_id})
  MERGE (t)-[r:SE_DESTINA_A]->(m)
  SET r.terminal_id = row.terminal_id,
      r.canal_pago = row.canal_pago,
      r.autorizacion = row.autorizacion,
      r.comision_pct = toFloat(row.comision_pct),
      r.fecha_hora = datetime(row.fecha_hora)
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Transaccion GENERA Alerta ----------
LOAD CSV WITH HEADERS FROM 'file:///rel_tx_genera_alerta.csv' AS row
CALL {
  WITH row
  MATCH (t:Transaccion {tx_id: row.tx_id})
  MATCH (a:Alerta {alerta_id: row.alerta_id})
  MERGE (t)-[r:GENERA]->(a)
  SET r.regla = row.regla,
      r.score_modelo = toFloat(row.score_modelo),
      r.version_modelo = row.version_modelo,
      r.fecha_deteccion = datetime(row.fecha_deteccion),
      r.auto_bloqueo = coalesce(toBoolean(row.auto_bloqueo), false)
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Alerta AFECTA_A Cliente ----------
LOAD CSV WITH HEADERS FROM 'file:///rel_alerta_afecta_cliente.csv' AS row
CALL {
  WITH row
  MATCH (a:Alerta {alerta_id: row.alerta_id})
  MATCH (c:Cliente {cliente_id: row.cliente_id})
  MERGE (a)-[r:AFECTA_A]->(c)
  SET r.rol_cliente = row.rol_cliente,
      r.criticidad = toInteger(row.criticidad),
      r.fecha_asignacion = date(row.fecha_asignacion),
      r.investigada = coalesce(toBoolean(row.investigada), false),
      r.analista = row.analista
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Comercio UBICADO_EN Ubicacion ----------
LOAD CSV WITH HEADERS FROM 'file:///rel_comercio_ubicado_en.csv' AS row
CALL {
  WITH row
  MATCH (m:Comercio {comercio_id: row.comercio_id})
  MATCH (u:Ubicacion {ubicacion_id: row.ubicacion_id})
  MERGE (m)-[r:UBICADO_EN]->(u)
  SET r.fecha_inicio = date(row.fecha_inicio),
      r.fecha_fin = CASE WHEN row.fecha_fin IS NULL OR trim(row.fecha_fin) = '' THEN NULL ELSE date(row.fecha_fin) END,
      r.es_principal = coalesce(toBoolean(row.es_principal), true),
      r.fuente_direccion = row.fuente_direccion,
      r.confianza_direccion = toFloat(row.confianza_direccion)
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Cliente COMPARTE_DISPOSITIVO Cliente ----------
LOAD CSV WITH HEADERS FROM 'file:///rel_cliente_comparte_dispositivo.csv' AS row
CALL {
  WITH row
  MATCH (c1:Cliente {cliente_id: row.cliente_origen_id})
  MATCH (c2:Cliente {cliente_id: row.cliente_destino_id})
  MERGE (c1)-[r:COMPARTE_DISPOSITIVO]->(c2)
  SET r.dispositivo_id_ref = row.dispositivo_id_ref,
      r.primera_coincidencia = datetime(row.primera_coincidencia),
      r.ultima_coincidencia = datetime(row.ultima_coincidencia),
      r.coincidencias = toInteger(row.coincidencias),
      r.score_riesgo_red = toFloat(row.score_riesgo_red)
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Cuenta VINCULADO_A Cuenta ----------
LOAD CSV WITH HEADERS FROM 'file:///rel_cuenta_vinculada_cuenta.csv' AS row
CALL {
  WITH row
  MATCH (c1:Cuenta {cuenta_id: row.cuenta_origen_id})
  MATCH (c2:Cuenta {cuenta_id: row.cuenta_destino_id})
  MERGE (c1)-[r:VINCULADO_A]->(c2)
  SET r.tipo_vinculo = row.tipo_vinculo,
      r.fecha_deteccion = date(row.fecha_deteccion),
      r.score_vinculo = toFloat(row.score_vinculo),
      r.evidencia = CASE WHEN row.evidencia IS NULL OR trim(row.evidencia) = '' THEN [] ELSE split(row.evidencia, '|') END,
      r.activo = coalesce(toBoolean(row.activo), true)
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Transaccion OCURRE_ANTES_DE Transaccion ----------
LOAD CSV WITH HEADERS FROM 'file:///rel_tx_ocurre_antes_tx.csv' AS row
CALL {
  WITH row
  MATCH (t1:Transaccion {tx_id: row.tx_origen_id})
  MATCH (t2:Transaccion {tx_id: row.tx_destino_id})
  MERGE (t1)-[r:OCURRE_ANTES_DE]->(t2)
  SET r.delta_segundos = toInteger(row.delta_segundos),
      r.misma_ip = coalesce(toBoolean(row.misma_ip), false),
      r.misma_ubicacion = coalesce(toBoolean(row.misma_ubicacion), false),
      r.patron = row.patron,
      r.score_patron = toFloat(row.score_patron)
} IN TRANSACTIONS OF 1000 ROWS;
