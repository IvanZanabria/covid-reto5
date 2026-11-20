// Algoritmos de grafos con Graph Data Science (plugin graph-data-science).
// Requiere en docker-compose:  NEO4J_dbms_security_procedures_unrestricted=gds.*,apoc.*

// G0. Proyectar el grafo en memoria (borrar antes si ya existe)
CALL gds.graph.drop('contagio', false) YIELD graphName;
CALL gds.graph.project('contagio', 'Caso', 'CONTAGIO_A')
YIELD graphName, nodeCount, relationshipCount;

// G1. PageRank: casos "críticos" por influencia en la propagación
CALL gds.pageRank.stream('contagio')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).id AS caso, score
ORDER BY score DESC LIMIT 10;

// G2. Componentes conexos: cada componente es un brote independiente
CALL gds.wcc.stream('contagio')
YIELD nodeId, componentId
RETURN componentId, count(*) AS tamano
ORDER BY tamano DESC LIMIT 10;

// G3. Distribución del tamaño de brotes (cuántos brotes de cada tamaño)
CALL gds.wcc.stream('contagio')
YIELD nodeId, componentId
WITH componentId, count(*) AS tamano
RETURN tamano, count(*) AS brotes
ORDER BY tamano DESC LIMIT 15;

// G4. Grado de salida: casos que más contagiaron directamente (superpropagadores)
MATCH (c:Caso)-[r:CONTAGIO_A]->()
RETURN c.id AS caso, count(r) AS contagios_directos
ORDER BY contagios_directos DESC LIMIT 10;

// G5. Liberar memoria al terminar
CALL gds.graph.drop('contagio') YIELD graphName;
