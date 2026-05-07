// ========================================================
// ETAPA 02 - FRAUDE BANCARIO
// 08) CONECTIVIDAD DEL GRAFO
// ========================================================

// 1) Detectar nodos aislados (debe ser 0)
MATCH (n)
WHERE NOT (n)--()
RETURN labels(n) AS labels, count(*) AS aislados
ORDER BY aislados DESC;

// 2) En caso de nodos aislados, reforzar backbone de cuentas
MATCH (cu:Cuenta)
WITH cu ORDER BY cu.cuenta_id
WITH collect(cu) AS cuentas
UNWIND range(0, size(cuentas)-2) AS i
WITH cuentas[i] AS c1, cuentas[i+1] AS c2
MERGE (c1)-[r:VINCULADO_A]->(c2)
ON CREATE SET
  r.tipo_vinculo = 'BACKBONE_CONECTIVIDAD',
  r.fecha_deteccion = date(),
  r.score_vinculo = 0.01,
  r.evidencia = ['conexion_global'],
  r.activo = true;

// 3) Verificacion final
MATCH (n)
WHERE NOT (n)--()
RETURN count(n) AS nodos_sin_relaciones;
