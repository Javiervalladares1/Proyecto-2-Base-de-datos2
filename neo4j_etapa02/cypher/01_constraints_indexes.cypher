// ========================================================
// ETAPA 02 - FRAUDE BANCARIO
// 01) CONSTRAINTS + INDICES
// Neo4j 5.x
// ========================================================

// ---------- CONSTRAINTS DE UNICIDAD ----------
CREATE CONSTRAINT cliente_id_unique IF NOT EXISTS
FOR (c:Cliente) REQUIRE c.cliente_id IS UNIQUE;

CREATE CONSTRAINT cuenta_id_unique IF NOT EXISTS
FOR (c:Cuenta) REQUIRE c.cuenta_id IS UNIQUE;

CREATE CONSTRAINT tx_id_unique IF NOT EXISTS
FOR (t:Transaccion) REQUIRE t.tx_id IS UNIQUE;

CREATE CONSTRAINT dispositivo_id_unique IF NOT EXISTS
FOR (d:Dispositivo) REQUIRE d.dispositivo_id IS UNIQUE;

CREATE CONSTRAINT ubicacion_id_unique IF NOT EXISTS
FOR (u:Ubicacion) REQUIRE u.ubicacion_id IS UNIQUE;

CREATE CONSTRAINT comercio_id_unique IF NOT EXISTS
FOR (m:Comercio) REQUIRE m.comercio_id IS UNIQUE;

CREATE CONSTRAINT alerta_id_unique IF NOT EXISTS
FOR (a:Alerta) REQUIRE a.alerta_id IS UNIQUE;

// ---------- INDICES DE NODOS ----------
CREATE INDEX idx_cliente_riesgo IF NOT EXISTS
FOR (c:Cliente) ON (c.riesgo_base);

CREATE INDEX idx_cliente_pais_segmento IF NOT EXISTS
FOR (c:Cliente) ON (c.pais_residencia, c.segmento);

CREATE INDEX idx_cuenta_estado IF NOT EXISTS
FOR (c:Cuenta) ON (c.estado);

CREATE INDEX idx_cuenta_score_riesgo IF NOT EXISTS
FOR (c:Cuenta) ON (c.score_riesgo);

CREATE INDEX idx_tx_fecha_hora IF NOT EXISTS
FOR (t:Transaccion) ON (t.fecha_hora);

CREATE INDEX idx_tx_monto IF NOT EXISTS
FOR (t:Transaccion) ON (t.monto);

CREATE INDEX idx_tx_score_fraude IF NOT EXISTS
FOR (t:Transaccion) ON (t.score_fraude);

CREATE INDEX idx_tx_internacional IF NOT EXISTS
FOR (t:Transaccion) ON (t.internacional);

CREATE INDEX idx_dispositivo_fingerprint IF NOT EXISTS
FOR (d:Dispositivo) ON (d.fingerprint);

CREATE INDEX idx_dispositivo_ip IF NOT EXISTS
FOR (d:Dispositivo) ON (d.ip_publica);

CREATE INDEX idx_ubicacion_pais_ciudad IF NOT EXISTS
FOR (u:Ubicacion) ON (u.pais, u.ciudad);

CREATE INDEX idx_comercio_mcc IF NOT EXISTS
FOR (m:Comercio) ON (m.categoria_mcc);

CREATE INDEX idx_comercio_riesgo IF NOT EXISTS
FOR (m:Comercio) ON (m.riesgo_comercio);

CREATE INDEX idx_alerta_estado IF NOT EXISTS
FOR (a:Alerta) ON (a.estado);

CREATE INDEX idx_alerta_severidad IF NOT EXISTS
FOR (a:Alerta) ON (a.severidad);

CREATE INDEX idx_alerta_fecha IF NOT EXISTS
FOR (a:Alerta) ON (a.fecha_creacion);

// ---------- INDICES DE RELACIONES ----------
CREATE INDEX idx_rel_genera_score IF NOT EXISTS
FOR ()-[r:GENERA]-() ON (r.score_modelo);

CREATE INDEX idx_rel_vinculado_score IF NOT EXISTS
FOR ()-[r:VINCULADO_A]-() ON (r.score_vinculo);

CREATE INDEX idx_rel_ocurre_delta IF NOT EXISTS
FOR ()-[r:OCURRE_ANTES_DE]-() ON (r.delta_segundos);
