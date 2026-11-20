// Ejecutar ANTES de cargar datos: sin estos índices, cada MATCH {id: ...} de
// la carga de aristas sería un escaneo completo (194 171 búsquedas lentas).
CREATE CONSTRAINT caso_id IF NOT EXISTS FOR (c:Caso) REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT municipio_cod IF NOT EXISTS FOR (m:Municipio) REQUIRE m.cod IS UNIQUE;
CREATE CONSTRAINT pais_nombre IF NOT EXISTS FOR (p:Pais) REQUIRE p.nombre IS UNIQUE;
CREATE INDEX caso_tipo IF NOT EXISTS FOR (c:Caso) ON (c.tipo_contagio);
