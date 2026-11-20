-- Consultas de demostración sobre Cassandra (keyspace covid_operational).
-- Cada una se resuelve leyendo UNA partición: por eso el modelo está hecho
-- a partir de las consultas.
--
-- Ejecutar:
--   docker compose exec -T cassandra cqlsh -f /dev/stdin < consultas/cassandra/consultas.cql
-- o abrir cqlsh:  docker compose exec cassandra cqlsh
--
-- Para activar el tiempo de cada consulta en cqlsh:  TRACING ON;

USE covid_operational;

-- Q1. Casos de Bogotá (11001) en octubre de 2020, del más reciente al más antiguo.
--     Partición = (cod_muni, anio_mes); el orden lo da el CLUSTERING ORDER.
SELECT id_caso, fecha_reporte, edad, sexo, tipo_contagio, estado
FROM casos_por_municipio_mes
WHERE cod_muni = '11001' AND anio_mes = '2020-10'
LIMIT 20;

-- Q2. Los 5 casos más recientes de Barranquilla (08001) en un mes.
SELECT id_caso, fecha_reporte, estado
FROM casos_por_municipio_mes
WHERE cod_muni = '08001' AND anio_mes = '2020-10'
LIMIT 5;

-- Q3. Datos de un caso por id: punto de entrada a la trazabilidad
--     (con este id se consulta luego el grafo en Neo4j).
--     Cambiar el id por uno que exista (por ejemplo, de Q1).
SELECT * FROM caso_por_id WHERE id_caso = '4553506';

-- Q4. Rango de fechas dentro de un mes (usa la clave de clustering).
SELECT id_caso, fecha_reporte, estado
FROM casos_por_municipio_mes
WHERE cod_muni = '11001' AND anio_mes = '2020-10'
  AND fecha_reporte >= '2020-10-10' AND fecha_reporte <= '2020-10-15';

-- NO soportado (y es a propósito): "todos los casos de un departamento" o
-- "casos con estado = Fallecido". Requerirían ALLOW FILTERING (escaneo de todo
-- el clúster). En este modelo esas preguntas se contestan en Spark/BigQuery.
