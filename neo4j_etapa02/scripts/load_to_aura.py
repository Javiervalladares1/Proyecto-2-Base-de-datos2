#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path
from typing import Dict, Iterable, List

from neo4j import GraphDatabase

ROOT = Path(__file__).resolve().parents[1]
CSV_DIR = ROOT / "csv"


def batched(rows: List[Dict[str, str]], size: int) -> Iterable[List[Dict[str, str]]]:
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


CONSTRAINTS_AND_INDEXES = [
    "CREATE CONSTRAINT cliente_id_unique IF NOT EXISTS FOR (c:Cliente) REQUIRE c.cliente_id IS UNIQUE",
    "CREATE CONSTRAINT cuenta_id_unique IF NOT EXISTS FOR (c:Cuenta) REQUIRE c.cuenta_id IS UNIQUE",
    "CREATE CONSTRAINT tx_id_unique IF NOT EXISTS FOR (t:Transaccion) REQUIRE t.tx_id IS UNIQUE",
    "CREATE CONSTRAINT dispositivo_id_unique IF NOT EXISTS FOR (d:Dispositivo) REQUIRE d.dispositivo_id IS UNIQUE",
    "CREATE CONSTRAINT ubicacion_id_unique IF NOT EXISTS FOR (u:Ubicacion) REQUIRE u.ubicacion_id IS UNIQUE",
    "CREATE CONSTRAINT comercio_id_unique IF NOT EXISTS FOR (m:Comercio) REQUIRE m.comercio_id IS UNIQUE",
    "CREATE CONSTRAINT alerta_id_unique IF NOT EXISTS FOR (a:Alerta) REQUIRE a.alerta_id IS UNIQUE",
    "CREATE INDEX idx_cliente_riesgo IF NOT EXISTS FOR (c:Cliente) ON (c.riesgo_base)",
    "CREATE INDEX idx_cliente_pais_segmento IF NOT EXISTS FOR (c:Cliente) ON (c.pais_residencia, c.segmento)",
    "CREATE INDEX idx_cuenta_estado IF NOT EXISTS FOR (c:Cuenta) ON (c.estado)",
    "CREATE INDEX idx_cuenta_score_riesgo IF NOT EXISTS FOR (c:Cuenta) ON (c.score_riesgo)",
    "CREATE INDEX idx_tx_fecha_hora IF NOT EXISTS FOR (t:Transaccion) ON (t.fecha_hora)",
    "CREATE INDEX idx_tx_monto IF NOT EXISTS FOR (t:Transaccion) ON (t.monto)",
    "CREATE INDEX idx_tx_score_fraude IF NOT EXISTS FOR (t:Transaccion) ON (t.score_fraude)",
    "CREATE INDEX idx_tx_internacional IF NOT EXISTS FOR (t:Transaccion) ON (t.internacional)",
    "CREATE INDEX idx_dispositivo_fingerprint IF NOT EXISTS FOR (d:Dispositivo) ON (d.fingerprint)",
    "CREATE INDEX idx_dispositivo_ip IF NOT EXISTS FOR (d:Dispositivo) ON (d.ip_publica)",
    "CREATE INDEX idx_ubicacion_pais_ciudad IF NOT EXISTS FOR (u:Ubicacion) ON (u.pais, u.ciudad)",
    "CREATE INDEX idx_comercio_mcc IF NOT EXISTS FOR (m:Comercio) ON (m.categoria_mcc)",
    "CREATE INDEX idx_comercio_riesgo IF NOT EXISTS FOR (m:Comercio) ON (m.riesgo_comercio)",
    "CREATE INDEX idx_alerta_estado IF NOT EXISTS FOR (a:Alerta) ON (a.estado)",
    "CREATE INDEX idx_alerta_severidad IF NOT EXISTS FOR (a:Alerta) ON (a.severidad)",
    "CREATE INDEX idx_alerta_fecha IF NOT EXISTS FOR (a:Alerta) ON (a.fecha_creacion)",
    "CREATE INDEX idx_rel_genera_score IF NOT EXISTS FOR ()-[r:GENERA]-() ON (r.score_modelo)",
    "CREATE INDEX idx_rel_vinculado_score IF NOT EXISTS FOR ()-[r:VINCULADO_A]-() ON (r.score_vinculo)",
    "CREATE INDEX idx_rel_ocurre_delta IF NOT EXISTS FOR ()-[r:OCURRE_ANTES_DE]-() ON (r.delta_segundos)",
]


NODE_LOADS = [
    (
        "clientes.csv",
        """
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
            c.ultima_actualizacion = datetime(row.ultima_actualizacion)
        FOREACH (_ IN CASE WHEN c.pep THEN [1] ELSE [] END | SET c:PEP)
        FOREACH (_ IN CASE WHEN c.riesgo_base >= 80 THEN [1] ELSE [] END | SET c:ClienteRiesgoAlto)
        """,
    ),
    (
        "cuentas.csv",
        """
        UNWIND $rows AS row
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
        """,
    ),
    (
        "dispositivos.csv",
        """
        UNWIND $rows AS row
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
        """,
    ),
    (
        "ubicaciones.csv",
        """
        UNWIND $rows AS row
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
        """,
    ),
    (
        "comercios.csv",
        """
        UNWIND $rows AS row
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
        """,
    ),
    (
        "transacciones.csv",
        """
        UNWIND $rows AS row
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
        """,
    ),
    (
        "alertas.csv",
        """
        UNWIND $rows AS row
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
        """,
    ),
]


REL_LOADS = [
    (
        "rel_cliente_posee_cuenta.csv",
        """
        UNWIND $rows AS row
        MATCH (c:Cliente {cliente_id: row.cliente_id})
        MATCH (cu:Cuenta {cuenta_id: row.cuenta_id})
        MERGE (c)-[r:POSEE]->(cu)
        SET r.fecha_inicio = date(row.fecha_inicio),
            r.tipo_titularidad = row.tipo_titularidad,
            r.porcentaje_propiedad = toFloat(row.porcentaje_propiedad),
            r.estado_relacion = row.estado_relacion,
            r.origen_onboarding = row.origen_onboarding
        """,
    ),
    (
        "rel_cliente_realiza_tx.csv",
        """
        UNWIND $rows AS row
        MATCH (c:Cliente {cliente_id: row.cliente_id})
        MATCH (t:Transaccion {tx_id: row.tx_id})
        MERGE (c)-[r:REALIZA]->(t)
        SET r.rol = row.rol,
            r.factor_autenticacion = row.factor_autenticacion,
            r.autenticacion_ok = coalesce(toBoolean(row.autenticacion_ok), false),
            r.ip_origen = row.ip_origen,
            r.confianza_sesion = toFloat(row.confianza_sesion)
        """,
    ),
    (
        "rel_tx_debita_cuenta.csv",
        """
        UNWIND $rows AS row
        MATCH (t:Transaccion {tx_id: row.tx_id})
        MATCH (cu:Cuenta {cuenta_id: row.cuenta_id})
        MERGE (t)-[r:DEBITA]->(cu)
        SET r.monto = toFloat(row.monto),
            r.moneda = row.moneda,
            r.saldo_previo = toFloat(row.saldo_previo),
            r.saldo_posterior = toFloat(row.saldo_posterior),
            r.timestamp_contable = datetime(row.timestamp_contable)
        """,
    ),
    (
        "rel_tx_acredita_cuenta.csv",
        """
        UNWIND $rows AS row
        MATCH (t:Transaccion {tx_id: row.tx_id})
        MATCH (cu:Cuenta {cuenta_id: row.cuenta_id})
        MERGE (t)-[r:ACREDITA]->(cu)
        SET r.monto = toFloat(row.monto),
            r.moneda = row.moneda,
            r.saldo_previo = toFloat(row.saldo_previo),
            r.saldo_posterior = toFloat(row.saldo_posterior),
            r.timestamp_contable = datetime(row.timestamp_contable)
        """,
    ),
    (
        "rel_cliente_usa_dispositivo.csv",
        """
        UNWIND $rows AS row
        MATCH (c:Cliente {cliente_id: row.cliente_id})
        MATCH (d:Dispositivo {dispositivo_id: row.dispositivo_id})
        MERGE (c)-[r:USA]->(d)
        SET r.primer_uso = date(row.primer_uso),
            r.ultimo_uso = datetime(row.ultimo_uso),
            r.veces_uso = toInteger(row.veces_uso),
            r.confianza_dispositivo = toFloat(row.confianza_dispositivo),
            r.activo = coalesce(toBoolean(row.activo), true)
        """,
    ),
    (
        "rel_tx_origina_ubicacion.csv",
        """
        UNWIND $rows AS row
        MATCH (t:Transaccion {tx_id: row.tx_id})
        MATCH (u:Ubicacion {ubicacion_id: row.ubicacion_id})
        MERGE (t)-[r:SE_ORIGINA_EN]->(u)
        SET r.fecha_hora = datetime(row.fecha_hora),
            r.precision_metros = toInteger(row.precision_metros),
            r.metodo_geolocalizacion = row.metodo_geolocalizacion,
            r.riesgo_geo = toInteger(row.riesgo_geo),
            r.es_vpn = coalesce(toBoolean(row.es_vpn), false)
        """,
    ),
    (
        "rel_tx_destina_comercio.csv",
        """
        UNWIND $rows AS row
        MATCH (t:Transaccion {tx_id: row.tx_id})
        MATCH (m:Comercio {comercio_id: row.comercio_id})
        MERGE (t)-[r:SE_DESTINA_A]->(m)
        SET r.terminal_id = row.terminal_id,
            r.canal_pago = row.canal_pago,
            r.autorizacion = row.autorizacion,
            r.comision_pct = toFloat(row.comision_pct),
            r.fecha_hora = datetime(row.fecha_hora)
        """,
    ),
    (
        "rel_tx_genera_alerta.csv",
        """
        UNWIND $rows AS row
        MATCH (t:Transaccion {tx_id: row.tx_id})
        MATCH (a:Alerta {alerta_id: row.alerta_id})
        MERGE (t)-[r:GENERA]->(a)
        SET r.regla = row.regla,
            r.score_modelo = toFloat(row.score_modelo),
            r.version_modelo = row.version_modelo,
            r.fecha_deteccion = datetime(row.fecha_deteccion),
            r.auto_bloqueo = coalesce(toBoolean(row.auto_bloqueo), false)
        """,
    ),
    (
        "rel_alerta_afecta_cliente.csv",
        """
        UNWIND $rows AS row
        MATCH (a:Alerta {alerta_id: row.alerta_id})
        MATCH (c:Cliente {cliente_id: row.cliente_id})
        MERGE (a)-[r:AFECTA_A]->(c)
        SET r.rol_cliente = row.rol_cliente,
            r.criticidad = toInteger(row.criticidad),
            r.fecha_asignacion = date(row.fecha_asignacion),
            r.investigada = coalesce(toBoolean(row.investigada), false),
            r.analista = row.analista
        """,
    ),
    (
        "rel_comercio_ubicado_en.csv",
        """
        UNWIND $rows AS row
        MATCH (m:Comercio {comercio_id: row.comercio_id})
        MATCH (u:Ubicacion {ubicacion_id: row.ubicacion_id})
        MERGE (m)-[r:UBICADO_EN]->(u)
        SET r.fecha_inicio = date(row.fecha_inicio),
            r.fecha_fin = CASE WHEN row.fecha_fin IS NULL OR trim(row.fecha_fin) = '' THEN NULL ELSE date(row.fecha_fin) END,
            r.es_principal = coalesce(toBoolean(row.es_principal), true),
            r.fuente_direccion = row.fuente_direccion,
            r.confianza_direccion = toFloat(row.confianza_direccion)
        """,
    ),
    (
        "rel_cliente_comparte_dispositivo.csv",
        """
        UNWIND $rows AS row
        MATCH (c1:Cliente {cliente_id: row.cliente_origen_id})
        MATCH (c2:Cliente {cliente_id: row.cliente_destino_id})
        MERGE (c1)-[r:COMPARTE_DISPOSITIVO]->(c2)
        SET r.dispositivo_id_ref = row.dispositivo_id_ref,
            r.primera_coincidencia = datetime(row.primera_coincidencia),
            r.ultima_coincidencia = datetime(row.ultima_coincidencia),
            r.coincidencias = toInteger(row.coincidencias),
            r.score_riesgo_red = toFloat(row.score_riesgo_red)
        """,
    ),
    (
        "rel_cuenta_vinculada_cuenta.csv",
        """
        UNWIND $rows AS row
        MATCH (c1:Cuenta {cuenta_id: row.cuenta_origen_id})
        MATCH (c2:Cuenta {cuenta_id: row.cuenta_destino_id})
        MERGE (c1)-[r:VINCULADO_A]->(c2)
        SET r.tipo_vinculo = row.tipo_vinculo,
            r.fecha_deteccion = date(row.fecha_deteccion),
            r.score_vinculo = toFloat(row.score_vinculo),
            r.evidencia = CASE WHEN row.evidencia IS NULL OR trim(row.evidencia) = '' THEN [] ELSE split(row.evidencia, '|') END,
            r.activo = coalesce(toBoolean(row.activo), true)
        """,
    ),
    (
        "rel_tx_ocurre_antes_tx.csv",
        """
        UNWIND $rows AS row
        MATCH (t1:Transaccion {tx_id: row.tx_origen_id})
        MATCH (t2:Transaccion {tx_id: row.tx_destino_id})
        MERGE (t1)-[r:OCURRE_ANTES_DE]->(t2)
        SET r.delta_segundos = toInteger(row.delta_segundos),
            r.misma_ip = coalesce(toBoolean(row.misma_ip), false),
            r.misma_ubicacion = coalesce(toBoolean(row.misma_ubicacion), false),
            r.patron = row.patron,
            r.score_patron = toFloat(row.score_patron)
        """,
    ),
]


def run_batch_query(session, query: str, rows: List[Dict[str, str]], batch_size: int, label: str) -> None:
    total = len(rows)
    if total == 0:
        print(f"[SKIP] {label}: archivo vacío")
        return

    done = 0
    for chunk in batched(rows, batch_size):
        session.run(query, rows=chunk).consume()
        done += len(chunk)
    print(f"[OK] {label}: {done} filas")


def run_constraints(session) -> None:
    for stmt in CONSTRAINTS_AND_INDEXES:
        session.run(stmt).consume()
    print(f"[OK] constraints+indices: {len(CONSTRAINTS_AND_INDEXES)}")


def maybe_wipe(session, do_wipe: bool) -> None:
    if not do_wipe:
        return
    print("[INFO] Limpiando grafo actual...")
    session.run("MATCH (n) DETACH DELETE n").consume()
    print("[OK] grafo limpio")


def verify_counts(session) -> None:
    node_queries = {
        "clientes": "MATCH (n:Cliente) RETURN count(n) AS c",
        "cuentas": "MATCH (n:Cuenta) RETURN count(n) AS c",
        "transacciones": "MATCH (n:Transaccion) RETURN count(n) AS c",
        "dispositivos": "MATCH (n:Dispositivo) RETURN count(n) AS c",
        "ubicaciones": "MATCH (n:Ubicacion) RETURN count(n) AS c",
        "comercios": "MATCH (n:Comercio) RETURN count(n) AS c",
        "alertas": "MATCH (n:Alerta) RETURN count(n) AS c",
    }

    rel_queries = {
        "posee": "MATCH ()-[r:POSEE]->() RETURN count(r) AS c",
        "realiza": "MATCH ()-[r:REALIZA]->() RETURN count(r) AS c",
        "debita": "MATCH ()-[r:DEBITA]->() RETURN count(r) AS c",
        "acredita": "MATCH ()-[r:ACREDITA]->() RETURN count(r) AS c",
        "usa": "MATCH ()-[r:USA]->() RETURN count(r) AS c",
        "se_origina_en": "MATCH ()-[r:SE_ORIGINA_EN]->() RETURN count(r) AS c",
        "se_destina_a": "MATCH ()-[r:SE_DESTINA_A]->() RETURN count(r) AS c",
        "genera": "MATCH ()-[r:GENERA]->() RETURN count(r) AS c",
        "afecta_a": "MATCH ()-[r:AFECTA_A]->() RETURN count(r) AS c",
        "ubicado_en": "MATCH ()-[r:UBICADO_EN]->() RETURN count(r) AS c",
        "comparte_dispositivo": "MATCH ()-[r:COMPARTE_DISPOSITIVO]->() RETURN count(r) AS c",
        "vinculado_a": "MATCH ()-[r:VINCULADO_A]->() RETURN count(r) AS c",
        "ocurre_antes_de": "MATCH ()-[r:OCURRE_ANTES_DE]->() RETURN count(r) AS c",
    }

    print("\n=== Conteos en Aura ===")
    for label, query in node_queries.items():
        count = session.run(query).single()["c"]
        print(f"{label}: {count}")

    for rel, query in rel_queries.items():
        count = session.run(query).single()["c"]
        print(f"{rel}: {count}")

    isolated = session.run("MATCH (n) WHERE NOT (n)--() RETURN count(n) AS aislados").single()["aislados"]
    print(f"nodos_aislados: {isolated}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Carga CSV local hacia Neo4j Aura usando driver Python")
    parser.add_argument("--uri", default=os.getenv("NEO4J_URI"), help="URI Neo4j, ej. neo4j+s://xxxx.databases.neo4j.io")
    parser.add_argument("--user", default=os.getenv("NEO4J_USERNAME") or os.getenv("NEO4J_USER"), help="Usuario Neo4j")
    parser.add_argument("--password", default=os.getenv("NEO4J_PASSWORD"), help="Password Neo4j")
    parser.add_argument("--database", default=os.getenv("NEO4J_DATABASE", "neo4j"), help="Base de datos")
    parser.add_argument("--batch-size", type=int, default=500, help="Tamaño de lote")
    parser.add_argument("--wipe", action="store_true", help="Elimina nodos existentes antes de cargar")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.uri or not args.user or not args.password:
        raise SystemExit("Faltan credenciales. Define --uri --user --password (y opcionalmente --database).")

    missing = [name for name, _, in (("csv", CSV_DIR),) if not CSV_DIR.exists()]
    if missing:
        raise SystemExit(f"No existe directorio CSV en {CSV_DIR}")

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    try:
        with driver.session(database=args.database) as session:
            maybe_wipe(session, args.wipe)
            run_constraints(session)

            print("\n=== Cargando nodos ===")
            for filename, query in NODE_LOADS:
                path = CSV_DIR / filename
                rows = read_csv_rows(path)
                run_batch_query(session, query, rows, args.batch_size, filename)

            print("\n=== Cargando relaciones ===")
            for filename, query in REL_LOADS:
                path = CSV_DIR / filename
                rows = read_csv_rows(path)
                run_batch_query(session, query, rows, args.batch_size, filename)

            verify_counts(session)

    finally:
        driver.close()


if __name__ == "__main__":
    main()
