// ========================================================
// ETAPA 02 - FRAUDE BANCARIO
// 04) CRUD COMPLETO DE NODOS Y RELACIONES
// ========================================================

// =========================
// CREATE
// =========================

// Nodo con 1 label
CREATE (c:Cliente {
  cliente_id: 'C999901',
  nombre_completo: 'Mario Ruiz',
  documento_id: 'DPI-1999010101010',
  fecha_nacimiento: date('1990-01-01'),
  fecha_alta: date('2026-04-21'),
  segmento: 'PERSONAL',
  ingresos_mensuales: 1800.0,
  pep: false,
  pais_residencia: 'GT',
  riesgo_base: 28.5,
  telefonos: ['+50255559901'],
  emails: ['mario.ruiz@correo.com'],
  productos_activos: ['AHORRO'],
  ultima_actualizacion: datetime()
});

// Nodo con multiples labels
CREATE (c2:Cliente:PEP:ClienteRiesgoAlto {
  cliente_id: 'C999902',
  nombre_completo: 'Laura Reyes',
  documento_id: 'DPI-2999020202020',
  fecha_nacimiento: date('1982-02-02'),
  fecha_alta: date('2026-04-21'),
  segmento: 'PREMIUM',
  ingresos_mensuales: 15000.0,
  pep: true,
  pais_residencia: 'GT',
  riesgo_base: 92.4,
  telefonos: ['+50255559902'],
  emails: ['laura.reyes@correo.com'],
  productos_activos: ['AHORRO', 'CORRIENTE', 'INVERSION'],
  ultima_actualizacion: datetime()
});

// Crear relacion con propiedades
MATCH (c:Cliente {cliente_id:'C999901'})
MATCH (cu:Cuenta {cuenta_id:'CU000001'})
CREATE (c)-[:POSEE {
  fecha_inicio: date('2026-04-21'),
  tipo_titularidad: 'PRINCIPAL',
  porcentaje_propiedad: 100.0,
  estado_relacion: 'ACTIVA',
  origen_onboarding: 'APP'
}]->(cu);

// =========================
// READ
// =========================

// Consulta por 1 label
MATCH (c:Cliente {cliente_id:'C999901'})
RETURN c;

// Consulta por multiples labels
MATCH (c:Cliente:PEP:ClienteRiesgoAlto)
RETURN c.cliente_id, c.nombre_completo, c.riesgo_base
ORDER BY c.riesgo_base DESC;

// Leer relaciones del cliente
MATCH (c:Cliente {cliente_id:'C999901'})-[r:POSEE]->(cu:Cuenta)
RETURN c.cliente_id, type(r) AS relacion, cu.cuenta_id, r.fecha_inicio, r.estado_relacion;

// =========================
// UPDATE
// =========================

// Actualizar propiedades y agregar label por riesgo
MATCH (c:Cliente {cliente_id:'C999901'})
SET c.riesgo_base = 88.1,
    c.ultima_actualizacion = datetime(),
    c:ClienteRiesgoAlto;

// Quitar label si baja riesgo
MATCH (c:Cliente {cliente_id:'C999901'})
WHERE c.riesgo_base < 80
REMOVE c:ClienteRiesgoAlto;

// Actualizar relacion
MATCH (c:Cliente {cliente_id:'C999901'})-[r:POSEE]->(cu:Cuenta {cuenta_id:'CU000001'})
SET r.estado_relacion = 'ACTIVA',
    r.porcentaje_propiedad = 100.0,
    r.origen_onboarding = 'SUCURSAL';

// =========================
// DELETE
// =========================

// Eliminar una relacion especifica
MATCH (:Cliente {cliente_id:'C999901'})-[r:POSEE]->(:Cuenta {cuenta_id:'CU000001'})
DELETE r;

// Eliminar nodo y sus relaciones
MATCH (c:Cliente {cliente_id:'C999901'})
DETACH DELETE c;

MATCH (c:Cliente {cliente_id:'C999902'})
DETACH DELETE c;
