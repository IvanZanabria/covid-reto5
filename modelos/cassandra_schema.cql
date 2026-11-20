CREATE KEYSPACE IF NOT EXISTS covid_operational
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE covid_operational;

-- Consulta 1: "casos de un municipio en un mes", del más reciente al más antiguo.
-- Partición = (cod_muni, anio_mes). Solo con cod_muni, Bogotá tendría los
-- 189.329 casos (30 %) en UNA partición, y con el dataset completo (6,4 M)
-- serían ~1,9 M. Agregar el mes reparte esa carga en particiones pequeñas.
CREATE TABLE IF NOT EXISTS casos_por_municipio_mes (
    cod_muni      text,
    anio_mes      text,   -- 'YYYY-MM' calculado desde fecha_reporte
    fecha_reporte date,
    id_caso       text,
    departamento  text,
    municipio     text,
    edad          int,
    sexo          text,
    tipo_contagio text,
    estado        text,
    ubicacion     text,
    PRIMARY KEY ((cod_muni, anio_mes), fecha_reporte, id_caso)
) WITH CLUSTERING ORDER BY (fecha_reporte DESC, id_caso ASC);

-- Consulta 2: "datos de un caso por su id" (punto de entrada para la trazabilidad).
CREATE TABLE IF NOT EXISTS caso_por_id (
    id_caso            text PRIMARY KEY,
    cod_muni           text,
    municipio          text,
    departamento       text,
    edad               int,
    sexo               text,
    tipo_contagio      text,
    estado             text,
    ubicacion          text,
    fecha_reporte      date,
    fecha_notificacion date
);
