#!/usr/bin/env bash
set -euo pipefail

# ========================================================
# ETAPA 02 - Carga automatizada a Neo4j
# Uso:
#   ./load_to_neo4j.sh \
#     --uri bolt://localhost:7687 \
#     --user neo4j \
#     --password 'tu_password' \
#     --database neo4j \
#     --import-dir '/ruta/al/import'
#
# También soporta variables:
#   NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE, NEO4J_IMPORT_DIR
# ========================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CSV_DIR="${ROOT_DIR}/csv"
CYPHER_DIR="${ROOT_DIR}/cypher"

URI="${NEO4J_URI:-bolt://localhost:7687}"
USER="${NEO4J_USER:-neo4j}"
PASSWORD="${NEO4J_PASSWORD:-}"
DATABASE="${NEO4J_DATABASE:-neo4j}"
IMPORT_DIR="${NEO4J_IMPORT_DIR:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --uri) URI="$2"; shift 2;;
    --user) USER="$2"; shift 2;;
    --password) PASSWORD="$2"; shift 2;;
    --database) DATABASE="$2"; shift 2;;
    --import-dir) IMPORT_DIR="$2"; shift 2;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "Argumento no reconocido: $1" >&2
      exit 1
      ;;
  esac
done

if ! command -v cypher-shell >/dev/null 2>&1; then
  echo "Error: cypher-shell no está instalado o no está en PATH." >&2
  exit 1
fi

if [[ -z "${PASSWORD}" ]]; then
  echo "Error: falta password. Usa --password o NEO4J_PASSWORD." >&2
  exit 1
fi

if [[ ! -d "${CSV_DIR}" ]]; then
  echo "Error: no existe directorio CSV: ${CSV_DIR}" >&2
  exit 1
fi

if [[ -n "${IMPORT_DIR}" ]]; then
  if [[ ! -d "${IMPORT_DIR}" ]]; then
    echo "Error: --import-dir no existe: ${IMPORT_DIR}" >&2
    exit 1
  fi
  echo "Copiando CSV a ${IMPORT_DIR} ..."
  cp "${CSV_DIR}"/*.csv "${IMPORT_DIR}/"
fi

run_cypher_file() {
  local file="$1"
  echo "Ejecutando $(basename "$file") ..."
  cypher-shell \
    --address "${URI}" \
    --username "${USER}" \
    --password "${PASSWORD}" \
    --database "${DATABASE}" \
    --file "${file}"
}

run_cypher_file "${CYPHER_DIR}/01_constraints_indexes.cypher"
run_cypher_file "${CYPHER_DIR}/02_load_nodes.cypher"
run_cypher_file "${CYPHER_DIR}/03_load_relationships.cypher"
run_cypher_file "${CYPHER_DIR}/08_connectivity_checks.cypher"

echo "Carga completada."
echo "Siguiente paso recomendado: ejecutar consultas de validación en 05, 06 y 07."
