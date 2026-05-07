// ========================================================
// ETAPA 02/PROYECTO - EVIDENCIAS PARA LA RUBRICA
// Ejecutar por secciones en Neo4j Browser.
// ========================================================

// 1) Caso de uso y labels disponibles
CALL db.labels()
YIELD label
RETURN collect(label) AS labels, count(label) AS total_labels;

// 2) Conteo por label y total de nodos
MATCH (n)
WITH labels(n)[0] AS label_principal, count(n) AS cantidad
RETURN label_principal, cantidad
ORDER BY cantidad DESC;

MATCH (n)
RETURN count(n) AS total_nodos;

// 3) Propiedades por cada label
MATCH (n:Cliente) RETURN labels(n) AS labels, keys(n) AS propiedades, n LIMIT 1;
MATCH (n:Cuenta) RETURN labels(n) AS labels, keys(n) AS propiedades, n LIMIT 1;
MATCH (n:Transaccion) RETURN labels(n) AS labels, keys(n) AS propiedades, n LIMIT 1;
MATCH (n:Dispositivo) RETURN labels(n) AS labels, keys(n) AS propiedades, n LIMIT 1;
MATCH (n:Ubicacion) RETURN labels(n) AS labels, keys(n) AS propiedades, n LIMIT 1;
MATCH (n:Comercio) RETURN labels(n) AS labels, keys(n) AS propiedades, n LIMIT 1;
MATCH (n:Alerta) RETURN labels(n) AS labels, keys(n) AS propiedades, n LIMIT 1;

// 4) Nodos con multiples labels
MATCH (n)
WHERE size(labels(n)) >= 2
RETURN labels(n) AS labels, count(n) AS cantidad
ORDER BY cantidad DESC;

// 5) Tipos de relaciones y conteos
MATCH ()-[r]->()
RETURN type(r) AS tipo_relacion, count(r) AS cantidad
ORDER BY cantidad DESC;

// 6) Propiedades por tipo de relacion
MATCH ()-[r]->()
WITH type(r) AS tipo_relacion, collect(keys(r))[0] AS propiedades
RETURN tipo_relacion, propiedades, size(propiedades) AS cantidad_propiedades
ORDER BY tipo_relacion;

// 7) Tipos de datos usados en propiedades
MATCH (c:Cliente)-[p:POSEE]->(cu:Cuenta)
MATCH (c)-[u:USA]->(d:Dispositivo)
MATCH (c)-[:REALIZA]->(t:Transaccion)-[:GENERA]->(a:Alerta)
RETURN
  valueType(c.nombre_completo) AS tipo_string,
  valueType(t.monto) AS tipo_float,
  valueType(a.severidad) AS tipo_integer,
  valueType(c.pep) AS tipo_boolean,
  valueType(c.telefonos) AS tipo_lista,
  valueType(c.fecha_alta) AS tipo_date,
  valueType(t.fecha_hora) AS tipo_datetime
LIMIT 1;

// 8) Grafo conexo: evidencias de conectividad por construccion
MATCH (n)
WHERE NOT (n)--()
RETURN count(n) AS nodos_aislados;

MATCH (:Cuenta)-[r:VINCULADO_A {tipo_vinculo:'BACKBONE_CONECTIVIDAD'}]->(:Cuenta)
RETURN count(r) AS relaciones_backbone_cuentas;

MATCH (c:Cliente)
WHERE NOT (c)-[:POSEE]->(:Cuenta)
RETURN count(c) AS clientes_sin_cuenta;

MATCH (t:Transaccion)
WHERE NOT (t)-[:DEBITA]->(:Cuenta) OR NOT (t)-[:ACREDITA]->(:Cuenta)
RETURN count(t) AS transacciones_sin_cuentas;

MATCH (a:Alerta)
WHERE NOT (:Transaccion)-[:GENERA]->(a)
RETURN count(a) AS alertas_sin_transaccion;

// 9) Visualizacion: consultar 1 nodo
MATCH (c:Cliente {cliente_id:'C000001'})
RETURN c;

// 10) Visualizacion: consultar muchos nodos con filtros
MATCH (c:Cliente)
WHERE c.riesgo_base >= 80 OR c.pep = true
RETURN c.cliente_id, c.nombre_completo, c.pep, c.riesgo_base
ORDER BY c.riesgo_base DESC
LIMIT 20;

// 11) Visualizacion: agregacion
MATCH (t:Transaccion)
RETURN t.canal AS canal, count(t) AS total_transacciones, round(avg(t.monto), 2) AS monto_promedio
ORDER BY total_transacciones DESC;

// ========================================================
// CRUD DEMO: ejecutar en orden. Usa IDs DEMO y al final limpia.
// ========================================================

// 12) CREATE: nodo con 1 label y 5+ propiedades
MERGE (c:Cliente {cliente_id:'CDEMO001'})
SET c.nombre_completo = 'Cliente Demo Uno',
    c.documento_id = 'DPI-DEMO-001',
    c.fecha_nacimiento = date('1992-03-10'),
    c.fecha_alta = date(),
    c.segmento = 'PERSONAL',
    c.ingresos_mensuales = 2500.0,
    c.pep = false,
    c.pais_residencia = 'GT',
    c.riesgo_base = 25.0,
    c.telefonos = ['+50255550001'],
    c.emails = ['demo1@correo.com'],
    c.productos_activos = ['AHORRO'],
    c.ultima_actualizacion = datetime()
RETURN c;

// 13) CREATE: nodo con 2+ labels y 5+ propiedades
MERGE (c:Cliente:PEP:ClienteRiesgoAlto {cliente_id:'CDEMO002'})
SET c.nombre_completo = 'Cliente Demo Dos',
    c.documento_id = 'DPI-DEMO-002',
    c.fecha_nacimiento = date('1980-08-21'),
    c.fecha_alta = date(),
    c.segmento = 'PREMIUM',
    c.ingresos_mensuales = 14500.0,
    c.pep = true,
    c.pais_residencia = 'GT',
    c.riesgo_base = 91.0,
    c.telefonos = ['+50255550002'],
    c.emails = ['demo2@correo.com'],
    c.productos_activos = ['AHORRO', 'INVERSION'],
    c.ultima_actualizacion = datetime()
RETURN c;

// 14) CREATE: relacion con propiedades
MATCH (c:Cliente {cliente_id:'CDEMO001'})
MATCH (cu:Cuenta {cuenta_id:'CU000001'})
MERGE (c)-[r:POSEE]->(cu)
SET r.fecha_inicio = date(),
    r.tipo_titularidad = 'PRINCIPAL',
    r.porcentaje_propiedad = 100.0,
    r.estado_relacion = 'ACTIVA',
    r.origen_onboarding = 'DEMO'
RETURN c, r, cu;

// 15) Gestion de propiedades en 1 nodo: agregar y actualizar
MATCH (c:Cliente {cliente_id:'CDEMO001'})
SET c.canal_preferido = 'APP',
    c.riesgo_base = 44.5,
    c.ultima_actualizacion = datetime()
RETURN c.cliente_id, c.canal_preferido, c.riesgo_base;

// 16) Gestion de propiedades en multiples nodos
MATCH (c:Cliente)
WHERE c.cliente_id IN ['CDEMO001', 'CDEMO002']
SET c.revision_manual = true,
    c.fecha_revision = date()
RETURN c.cliente_id, c.revision_manual, c.fecha_revision;

// 17) Eliminar propiedades de 1 nodo
MATCH (c:Cliente {cliente_id:'CDEMO001'})
REMOVE c.canal_preferido
RETURN c.cliente_id, keys(c) AS propiedades_actuales;

// 18) Eliminar propiedades de multiples nodos
MATCH (c:Cliente)
WHERE c.cliente_id IN ['CDEMO001', 'CDEMO002']
REMOVE c.revision_manual, c.fecha_revision
RETURN c.cliente_id, keys(c) AS propiedades_actuales;

// 19) Gestion de propiedades en 1 relacion
MATCH (:Cliente {cliente_id:'CDEMO001'})-[r:POSEE]->(:Cuenta {cuenta_id:'CU000001'})
SET r.validada_por = 'analista_demo',
    r.score_confianza = 0.98
RETURN r;

// 20) Gestion de propiedades en multiples relaciones
MATCH (:Cliente)-[r:POSEE]->(:Cuenta)
WHERE r.origen_onboarding = 'DEMO'
SET r.lote_demo = 'rubrica'
RETURN count(r) AS relaciones_actualizadas;

// 21) Eliminar propiedades de relacion
MATCH (:Cliente {cliente_id:'CDEMO001'})-[r:POSEE]->(:Cuenta {cuenta_id:'CU000001'})
REMOVE r.validada_por, r.score_confianza, r.lote_demo
RETURN r;

// 22) Eliminar 1 relacion
MATCH (:Cliente {cliente_id:'CDEMO001'})-[r:POSEE]->(:Cuenta {cuenta_id:'CU000001'})
DELETE r;

// 23) Eliminar multiples relaciones demo si existieran
MATCH (:Cliente)-[r]->()
WHERE startNode(r).cliente_id IN ['CDEMO001', 'CDEMO002']
DELETE r;

// 24) Eliminar 1 nodo
MATCH (c:Cliente {cliente_id:'CDEMO001'})
DETACH DELETE c;

// 25) Eliminar multiples nodos
MATCH (c:Cliente)
WHERE c.cliente_id IN ['CDEMO002']
DETACH DELETE c;

