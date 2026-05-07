#!/usr/bin/env python3
from neo4j import GraphDatabase
import os

uri = os.getenv('NEO4J_URI')
user = os.getenv('NEO4J_USERNAME') or os.getenv('NEO4J_USER')
password = os.getenv('NEO4J_PASSWORD')
database = os.getenv('NEO4J_DATABASE', 'neo4j')

if not all([uri, user, password]):
    raise SystemExit('Faltan credenciales en variables de entorno')

driver = GraphDatabase.driver(uri, auth=(user, password))

node_queries = {
    'clientes': 'MATCH (n:Cliente) RETURN count(n) AS c',
    'cuentas': 'MATCH (n:Cuenta) RETURN count(n) AS c',
    'transacciones': 'MATCH (n:Transaccion) RETURN count(n) AS c',
    'dispositivos': 'MATCH (n:Dispositivo) RETURN count(n) AS c',
    'ubicaciones': 'MATCH (n:Ubicacion) RETURN count(n) AS c',
    'comercios': 'MATCH (n:Comercio) RETURN count(n) AS c',
    'alertas': 'MATCH (n:Alerta) RETURN count(n) AS c',
}

rel_queries = {
    'POSEE': 'MATCH ()-[r:POSEE]->() RETURN count(r) AS c',
    'REALIZA': 'MATCH ()-[r:REALIZA]->() RETURN count(r) AS c',
    'DEBITA': 'MATCH ()-[r:DEBITA]->() RETURN count(r) AS c',
    'ACREDITA': 'MATCH ()-[r:ACREDITA]->() RETURN count(r) AS c',
    'USA': 'MATCH ()-[r:USA]->() RETURN count(r) AS c',
    'SE_ORIGINA_EN': 'MATCH ()-[r:SE_ORIGINA_EN]->() RETURN count(r) AS c',
    'SE_DESTINA_A': 'MATCH ()-[r:SE_DESTINA_A]->() RETURN count(r) AS c',
    'GENERA': 'MATCH ()-[r:GENERA]->() RETURN count(r) AS c',
    'AFECTA_A': 'MATCH ()-[r:AFECTA_A]->() RETURN count(r) AS c',
    'UBICADO_EN': 'MATCH ()-[r:UBICADO_EN]->() RETURN count(r) AS c',
    'COMPARTE_DISPOSITIVO': 'MATCH ()-[r:COMPARTE_DISPOSITIVO]->() RETURN count(r) AS c',
    'VINCULADO_A': 'MATCH ()-[r:VINCULADO_A]->() RETURN count(r) AS c',
    'OCURRE_ANTES_DE': 'MATCH ()-[r:OCURRE_ANTES_DE]->() RETURN count(r) AS c',
}

with driver.session(database=database) as session:
    print('=== NODOS ===')
    for k, q in node_queries.items():
        print(f'{k}: {session.run(q).single()["c"]}')
    print('\n=== RELACIONES ===')
    for k, q in rel_queries.items():
        print(f'{k}: {session.run(q).single()["c"]}')
    aislados = session.run('MATCH (n) WHERE NOT (n)--() RETURN count(n) AS c').single()['c']
    print(f'\nnodos_aislados: {aislados}')

driver.close()
