// Consultas de trazabilidad (coinciden con las capturas ejecutadas).
// Cada caso tiene a lo sumo un "padre", así que el grafo es un bosque y los
// recorridos sin tope de profundidad son baratos.
// NOTA: las aristas CONTAGIO_A son simuladas (ver README): el alcance y la
// profundidad dependen del método de generación, no de datos epidemiológicos.
// Ejecutar con un id real:  :param id => '4553506'
// (o reemplazar $id por el literal).

// T1. HACIA ATRÁS: ¿de dónde viene este caso? Origen y cadena completa.
//     (captura: id 4553506 -> origen 1831382, 30 saltos, 56 ms)
MATCH p = (o:Caso)-[:CONTAGIO_A*]->(:Caso {id: $id})
WHERE NOT ()-[:CONTAGIO_A]->(o)
RETURN o.id AS origen, length(p) AS saltos, [n IN nodes(p) | n.id] AS cadena;

// T1b. Cadenas más profundas con tope de 30 saltos.
//      OJO: en la captura el final de la consulta estaba cortado; se asume
//      ORDER BY profundidad DESC LIMIT 5 (devolvió 5 filas). Verificar.
MATCH p = (o:Caso)-[:CONTAGIO_A*1..30]->(d:Caso)
WHERE NOT ()-[:CONTAGIO_A]->(o)
RETURN d.id AS caso_final, o.id AS origen, length(p) AS profundidad
ORDER BY profundidad DESC LIMIT 5;

// T2. HACIA ADELANTE: ¿a cuántos casos llegó este origen?
//     (captura: 90317 -> 184 en 148 ms; 1831382 -> 189 en 211 ms)
MATCH (:Caso {id: $id})-[:CONTAGIO_A*]->(d:Caso)
RETURN count(DISTINCT d) AS alcance;

// T3. Afectados por municipio a partir de un origen (hacia adelante)
//     (captura: 1831382 -> 189 casos, todos en SAHAGUN, porque las aristas
//     simuladas no cruzan municipios)
MATCH (:Caso {id: $id})-[:CONTAGIO_A*]->(d:Caso)-[:OCURRIO_EN]->(m:Municipio)
RETURN m.nombre AS municipio, count(DISTINCT d) AS casos
ORDER BY casos DESC;

// T4. Visualización del camino (vista de grafo; captura: 31 nodos, 30 relaciones)
MATCH p = (o:Caso)-[:CONTAGIO_A*]->(:Caso {id: $id})
WHERE NOT ()-[:CONTAGIO_A]->(o)
RETURN p;

// T5. Orígenes con mayor alcance, SIN tope de profundidad (1,6 s)
//     (captura: 1088963 -> 258, 1831382 -> 189, 90317 -> 184, ...)
MATCH (o:Caso)-[:CONTAGIO_A*]->(d:Caso)
WHERE NOT ()-[:CONTAGIO_A]->(o)
RETURN o.id AS origen, count(DISTINCT d) AS alcance
ORDER BY alcance DESC LIMIT 10;

// T5b. Misma consulta con tope de 15 saltos (1,4 s). Compararla con T5 muestra
//      cuánto alcance se pierde al limitar la profundidad.
MATCH (o:Caso)-[:CONTAGIO_A*1..15]->(d:Caso)
WHERE NOT ()-[:CONTAGIO_A]->(o)
RETURN o.id AS origen, count(DISTINCT d) AS alcance
ORDER BY alcance DESC LIMIT 10;

// T6. Casos importados: ¿qué países originaron más casos y cuántos derivaron?
//     OPTIONAL MATCH conserva los países cuyos importados no contagiaron a nadie.
//     (captura: EE. UU. 134 importados / 118 derivados; España 49 / 163; ...)
MATCH (p:Pais)-[:ORIGEN_DE]->(c:Caso)
OPTIONAL MATCH (c)-[:CONTAGIO_A*]->(d:Caso)
RETURN p.nombre AS pais, count(DISTINCT c) AS casos_importados, count(DISTINCT d) AS casos_derivados
ORDER BY casos_importados DESC LIMIT 10;

// T7. Tiempo entre notificaciones padre -> hijo (NO es el retraso de reporte
//     de BigQuery). (captura: promedio 8,31 · mediana 0 · máximo 670 días)
MATCH ()-[r:CONTAGIO_A]->()
RETURN avg(r.dias) AS dias_promedio,
       percentileCont(r.dias, 0.5) AS mediana_dias,
       max(r.dias) AS max_dias;
