// Carga de los CSV generados por Spark (data/processed, montada como carpeta
// import de Neo4j). Orden obligatorio: municipios -> casos -> paises -> aristas.
// Ejecutar cada bloque por separado en Neo4j Browser (los que empiezan con
// :auto usan transacciones por lotes).
//
// Modelo: (Caso)-[:OCURRIO_EN]->(Municipio), (Pais)-[:ORIGEN_DE]->(Caso),
//         (Caso)-[:CONTAGIO_A {dias}]->(Caso)
// Los nombres de relaciones coinciden con los de las capturas ejecutadas.

// 1. Municipios (1 112)
LOAD CSV WITH HEADERS FROM 'file:///nodos_municipios.csv' AS row
MERGE (m:Municipio {cod: row.cod_muni})
SET m.nombre = row.municipio,
    m.cod_depto = row.cod_depto,
    m.departamento = row.departamento,
    m.total_casos = toInteger(row.total_casos);

// 2. Casos (639 097) y su municipio
:auto LOAD CSV WITH HEADERS FROM 'file:///nodos_casos.csv' AS row
CALL {
  WITH row
  CREATE (c:Caso {
    id: row.id,
    edad: toInteger(row.edad),
    sexo: row.sexo,
    tipo_contagio: row.tipo_contagio,
    estado: row.estado,
    ubicacion: row.ubicacion,
    fecha_notificacion: date(row.fecha_notificacion)
  })
  WITH c, row
  MATCH (m:Municipio {cod: row.cod_muni})
  CREATE (c)-[:OCURRIO_EN]->(m)
} IN TRANSACTIONS OF 10000 ROWS;

// 3. Países de origen de los casos importados (41 países, 368 relaciones)
LOAD CSV WITH HEADERS FROM 'file:///origen_importados.csv' AS row
MERGE (p:Pais {nombre: row.pais})
WITH p, row
MATCH (c:Caso {id: row.caso_id})
CREATE (p)-[:ORIGEN_DE]->(c);

// 4. Aristas de contagio (194 171). 'dias' = días entre las notificaciones
//    del caso padre y del caso hijo. Las aristas son SIMULADAS (ver README).
:auto LOAD CSV WITH HEADERS FROM 'file:///aristas_contagio.csv' AS row
CALL {
  WITH row
  MATCH (a:Caso {id: row.origen_id}), (b:Caso {id: row.destino_id})
  CREATE (a)-[:CONTAGIO_A {dias: toInteger(row.dias)}]->(b)
} IN TRANSACTIONS OF 10000 ROWS;

// 5. Verificación (deben dar 639097, 194171, 41 y 1112)
MATCH (c:Caso) RETURN count(c) AS casos;
MATCH ()-[r:CONTAGIO_A]->() RETURN count(r) AS aristas;
MATCH (p:Pais) RETURN count(p) AS paises;
MATCH (m:Municipio) RETURN count(m) AS municipios;
