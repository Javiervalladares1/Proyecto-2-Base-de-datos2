# App Frontend - Fraude Bancario Neo4j

Aplicacion full-stack para demostrar la rubrica y los puntos extra del proyecto.

## Ejecutar

```bash
cd neo4j_etapa02/app
npm install

NEO4J_URI='neo4j+ssc://47541692.databases.neo4j.io' \
NEO4J_USERNAME='47541692' \
NEO4J_PASSWORD='<password>' \
NEO4J_DATABASE='47541692' \
PORT=5174 \
npm run dev
```

Abrir:

```text
http://localhost:5174
```

## Pantallas

- Panel: metricas, labels, relaciones, nodos aislados y riesgo.
- Grafo: visualizacion SVG de una muestra o de un cliente especifico.
- Fraude: consultas Cypher de deteccion y scoring de data science.
- CRUD: creacion de nodos con 1 label, multiples labels, relaciones y actualizacion.
- CSV: carga de clientes desde archivo CSV.
- Rubrica: checklist de cumplimiento para la presentacion.

## Variables

Usa `.env.example` como referencia. No es necesario guardar credenciales en archivo; puedes pasarlas como variables al ejecutar.

