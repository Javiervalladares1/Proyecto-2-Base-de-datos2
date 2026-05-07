// ========================================================
// ETAPA 02 - FRAUDE BANCARIO
// 02) CARGA DE NODOS DESDE CSV
// Requiere que los CSV estén en el directorio import de Neo4j
// ========================================================

// ---------- Cliente ----------
LOAD CSV WITH HEADERS FROM 'file:///clientes.csv' AS row
CALL {
  WITH row
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
      c.ultima_actualizacion = datetime(row.ultima_actualizacion)

  FOREACH (_ IN CASE WHEN c.pep THEN [1] ELSE [] END | SET c:PEP)
  FOREACH (_ IN CASE WHEN c.riesgo_base >= 80 THEN [1] ELSE [] END | SET c:ClienteRiesgoAlto)
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Cuenta ----------
LOAD CSV WITH HEADERS FROM 'file:///cuentas.csv' AS row
CALL {
  WITH row
  MERGE (cu:Cuenta {cuenta_id: row.cuenta_id})
  SET cu.iban = row.iban,
      cu.tipo_cuenta = row.tipo_cuenta,
      cu.moneda_base = row.moneda_base,
      cu.fecha_apertura = date(row.fecha_apertura),
      cu.estado = row.estado,
      cu.saldo_actual = toFloat(row.saldo_actual),
      cu.limite_diario = toFloat(row.limite_diario),
      cu.es_conjunta = coalesce(toBoolean(row.es_conjunta), false),
      cu.canales_habilitados = CASE WHEN row.canales_habilitados IS NULL OR trim(row.canales_habilitados) = '' THEN [] ELSE split(row.canales_habilitados, '|') END,
      cu.pais_apertura = row.pais_apertura,
      cu.score_riesgo = toFloat(row.score_riesgo),
      cu.ultima_actividad = datetime(row.ultima_actividad)
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Dispositivo ----------
LOAD CSV WITH HEADERS FROM 'file:///dispositivos.csv' AS row
CALL {
  WITH row
  MERGE (d:Dispositivo {dispositivo_id: row.dispositivo_id})
  SET d.fingerprint = row.fingerprint,
      d.tipo_dispositivo = row.tipo_dispositivo,
      d.sistema_operativo = row.sistema_operativo,
      d.app_version = row.app_version,
      d.ip_publica = row.ip_publica,
      d.imei_hash = row.imei_hash,
      d.rooteado = coalesce(toBoolean(row.rooteado), false),
      d.emulador = coalesce(toBoolean(row.emulador), false),
      d.idiomas = CASE WHEN row.idiomas IS NULL OR trim(row.idiomas) = '' THEN [] ELSE split(row.idiomas, '|') END,
      d.fecha_registro = date(row.fecha_registro),
      d.ultima_actividad = datetime(row.ultima_actividad),
      d.confianza = toFloat(row.confianza)

  FOREACH (_ IN CASE WHEN d.rooteado OR d.emulador THEN [1] ELSE [] END | SET d:DispositivoComprometido)
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Ubicacion ----------
LOAD CSV WITH HEADERS FROM 'file:///ubicaciones.csv' AS row
CALL {
  WITH row
  MERGE (u:Ubicacion {ubicacion_id: row.ubicacion_id})
  SET u.pais = row.pais,
      u.ciudad = row.ciudad,
      u.region = row.region,
      u.codigo_postal = row.codigo_postal,
      u.latitud = toFloat(row.latitud),
      u.longitud = toFloat(row.longitud),
      u.zona_horaria = row.zona_horaria,
      u.es_frontera = coalesce(toBoolean(row.es_frontera), false),
      u.nivel_riesgo_geo = toInteger(row.nivel_riesgo_geo),
      u.coordenadas_vecinas = CASE WHEN row.coordenadas_vecinas IS NULL OR trim(row.coordenadas_vecinas) = '' THEN [] ELSE split(row.coordenadas_vecinas, '|') END,
      u.fecha_actualizacion = date(row.fecha_actualizacion),
      u.geo = point({latitude: toFloat(row.latitud), longitude: toFloat(row.longitud)})
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Comercio ----------
LOAD CSV WITH HEADERS FROM 'file:///comercios.csv' AS row
CALL {
  WITH row
  MERGE (m:Comercio {comercio_id: row.comercio_id})
  SET m.nombre_comercio = row.nombre_comercio,
      m.categoria_mcc = row.categoria_mcc,
      m.industria = row.industria,
      m.pais = row.pais,
      m.ciudad = row.ciudad,
      m.riesgo_comercio = toFloat(row.riesgo_comercio),
      m.antiguedad_meses = toInteger(row.antiguedad_meses),
      m.blacklist = coalesce(toBoolean(row.blacklist), false),
      m.metodos_pago_aceptados = CASE WHEN row.metodos_pago_aceptados IS NULL OR trim(row.metodos_pago_aceptados) = '' THEN [] ELSE split(row.metodos_pago_aceptados, '|') END,
      m.fecha_alta = date(row.fecha_alta),
      m.rating_promedio = toFloat(row.rating_promedio),
      m.url = row.url

  FOREACH (_ IN CASE WHEN m.blacklist OR m.riesgo_comercio >= 80 THEN [1] ELSE [] END | SET m:ComercioRiesgoso)
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Transaccion ----------
LOAD CSV WITH HEADERS FROM 'file:///transacciones.csv' AS row
CALL {
  WITH row
  MERGE (t:Transaccion {tx_id: row.tx_id})
  SET t.referencia_externa = row.referencia_externa,
      t.fecha_hora = datetime(row.fecha_hora),
      t.fecha_valor = date(row.fecha_valor),
      t.monto = toFloat(row.monto),
      t.moneda = row.moneda,
      t.tipo_tx = row.tipo_tx,
      t.canal = row.canal,
      t.estado = row.estado,
      t.internacional = coalesce(toBoolean(row.internacional), false),
      t.contracargo = coalesce(toBoolean(row.contracargo), false),
      t.velocity_1h = toInteger(row.velocity_1h),
      t.etiquetas = CASE WHEN row.etiquetas IS NULL OR trim(row.etiquetas) = '' THEN [] ELSE split(row.etiquetas, '|') END,
      t.score_fraude = toFloat(row.score_fraude)

  FOREACH (_ IN CASE WHEN t.internacional THEN [1] ELSE [] END | SET t:TransaccionInternacional)
  FOREACH (_ IN CASE WHEN t.score_fraude >= 0.85 OR t.contracargo THEN [1] ELSE [] END | SET t:TransaccionSospechosa)
} IN TRANSACTIONS OF 1000 ROWS;

// ---------- Alerta ----------
LOAD CSV WITH HEADERS FROM 'file:///alertas.csv' AS row
CALL {
  WITH row
  MERGE (a:Alerta {alerta_id: row.alerta_id})
  SET a.tipo_alerta = row.tipo_alerta,
      a.severidad = toInteger(row.severidad),
      a.score_alerta = toFloat(row.score_alerta),
      a.estado = row.estado,
      a.descripcion = row.descripcion,
      a.fecha_creacion = datetime(row.fecha_creacion),
      a.fecha_cierre = CASE WHEN row.fecha_cierre IS NULL OR trim(row.fecha_cierre) = '' THEN NULL ELSE date(row.fecha_cierre) END,
      a.es_falso_positivo = coalesce(toBoolean(row.es_falso_positivo), false),
      a.reglas_disparadas = CASE WHEN row.reglas_disparadas IS NULL OR trim(row.reglas_disparadas) = '' THEN [] ELSE split(row.reglas_disparadas, '|') END,
      a.acciones_recomendadas = CASE WHEN row.acciones_recomendadas IS NULL OR trim(row.acciones_recomendadas) = '' THEN [] ELSE split(row.acciones_recomendadas, '|') END,
      a.analista_asignado = row.analista_asignado

  FOREACH (_ IN CASE WHEN a.severidad >= 4 THEN [1] ELSE [] END | SET a:AlertaCritica)
} IN TRANSACTIONS OF 1000 ROWS;
