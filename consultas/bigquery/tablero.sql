-- Tablero de indicadores en BigQuery (Reto 5): alcance, tiempo de detección
-- y nodos críticos. Reemplazar covid-reto5 por el id del proyecto de Google Cloud.
-- Ejecutar cada consulta por separado en la consola de BigQuery.
-- La tabla casos está agrupada (clustering) por fecha, departamento y municipio:
-- filtrar por esas columnas reduce los bytes leídos (y por eso el costo).

-- ============================================================ 1. ALCANCE
-- 1a. Resumen general de la propagación simulada
SELECT
  (SELECT COUNT(*) FROM `covid-reto5.covid_trazabilidad.casos`)             AS casos_totales,
  (SELECT COUNT(*) FROM `covid-reto5.covid_trazabilidad.aristas_contagio`)  AS contagios_trazados,
  (SELECT COUNT(DISTINCT origen_id) FROM `covid-reto5.covid_trazabilidad.aristas_contagio`) AS casos_que_contagiaron;

-- 1b. Casos que contagiaron a otros, agrupados por departamento del caso origen
SELECT c.departamento,
       COUNT(DISTINCT a.origen_id) AS origenes,
       COUNT(*)                    AS contagios_directos
FROM `covid-reto5.covid_trazabilidad.aristas_contagio` a
JOIN `covid-reto5.covid_trazabilidad.casos` c ON c.id_caso = a.origen_id
GROUP BY c.departamento
ORDER BY contagios_directos DESC
LIMIT 10;

-- ============================================================ 2. TIEMPO DE DETECCIÓN
-- Días entre la notificación del caso y su reporte en la web.
-- 2a. Por tipo de contagio
SELECT tipo_contagio,
       COUNT(*)                          AS casos,
       ROUND(AVG(dias_deteccion), 2)     AS dias_promedio,
       APPROX_QUANTILES(dias_deteccion, 100)[OFFSET(50)] AS mediana,
       APPROX_QUANTILES(dias_deteccion, 100)[OFFSET(90)] AS p90
FROM `covid-reto5.covid_trazabilidad.casos`
GROUP BY tipo_contagio
ORDER BY casos DESC;

-- 2b. Evolución mensual del tiempo de detección (filtra por fecha: usa el clustering)
SELECT FORMAT_DATE('%Y-%m', fecha_notificacion) AS mes,
       COUNT(*)                                  AS casos,
       ROUND(AVG(dias_deteccion), 2)             AS dias_promedio
FROM `covid-reto5.covid_trazabilidad.casos`
WHERE fecha_notificacion BETWEEN '2020-03-01' AND '2021-12-31'
GROUP BY mes
ORDER BY mes;

-- ============================================================ 3. NODOS CRÍTICOS
-- 3a. Municipios críticos: más casos y mayor letalidad
SELECT departamento, municipio,
       COUNT(*)                                          AS casos,
       COUNTIF(estado = 'Fallecido')                     AS fallecidos,
       ROUND(100 * COUNTIF(estado = 'Fallecido') / COUNT(*), 2) AS letalidad_pct
FROM `covid-reto5.covid_trazabilidad.casos`
GROUP BY departamento, municipio
HAVING casos > 1000
ORDER BY casos DESC
LIMIT 15;

-- 3b. Superpropagadores: casos con más contagios directos
SELECT a.origen_id AS caso, COUNT(*) AS contagios_directos,
       ANY_VALUE(c.municipio) AS municipio, ANY_VALUE(c.edad) AS edad
FROM `covid-reto5.covid_trazabilidad.aristas_contagio` a
JOIN `covid-reto5.covid_trazabilidad.casos` c ON c.id_caso = a.origen_id
GROUP BY a.origen_id
ORDER BY contagios_directos DESC
LIMIT 10;

-- 3c. Países de origen de los casos importados
SELECT pais_origen, COUNT(*) AS casos_importados
FROM `covid-reto5.covid_trazabilidad.casos`
WHERE pais_origen IS NOT NULL
GROUP BY pais_origen
ORDER BY casos_importados DESC
LIMIT 10;
