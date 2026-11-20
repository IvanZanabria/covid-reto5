"""
ETL con Spark - Reto 5 (trazabilidad y propagación COVID-19 Colombia)

Hace tres cosas:
  1. Limpia los casos (códigos con puntos, mayúsculas inconsistentes, edad en
     meses/días, estado vacío) y los guarda en Parquet particionado por departamento.
  2. Genera los CSV de nodos que se cargan en Neo4j.
  3. Simula las aristas de contagio (el dataset NO dice quién contagió a quién).

Ejecutar desde la raíz del proyecto (con los contenedores arriba):
  docker compose exec spark /opt/spark/bin/spark-submit --master "local[*]" \
      --driver-memory 4g /app/procesamiento/spark_etl_propagacion.py
"""
import glob
import os
import shutil
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

ENTRADA = "data/raw/casos_covid_600k.csv"
PARQUET = "data/processed/casos_parquet"
SALIDA = "data/processed"

# Cada caso "Relacionado" se enlaza a uno de los K casos anteriores del MISMO
# municipio. La elección usa un hash del id, así que es reproducible.
K_VENTANA = 30

spark = (
    SparkSession.builder.appName("CovidETLPropagacion")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
t0 = time.perf_counter()


def fecha(nombre_columna):
    """'2020-10-28 00:00:00' -> date. Toma solo los 10 primeros caracteres."""
    return F.to_date(F.substring(F.col(nombre_columna), 1, 10), "yyyy-MM-dd")


def exportar_csv(df, nombre):
    """Spark escribe una carpeta con part-xxxx; aquí lo dejamos como un único
    archivo <nombre>.csv, que es lo que necesita LOAD CSV de Neo4j."""
    tmp = f"{SALIDA}/_tmp_{nombre}"
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(tmp)
    parte = glob.glob(f"{tmp}/part-*.csv")[0]
    os.replace(parte, f"{SALIDA}/{nombre}.csv")
    shutil.rmtree(tmp)


# ---------------------------------------------------------------- 1. LECTURA
# Todo como texto (inferSchema=false): si Spark infiere tipos, un código como
# "11.001" se vuelve número decimal y se pierden ceros ("08001" -> 8.001).
print("Cargando dataset...")
raw = spark.read.option("header", "true").option("inferSchema", "false").csv(ENTRADA)
n_raw = raw.count()
print(f"Registros leídos: {n_raw:,}")

# ---------------------------------------------------------------- 2. LIMPIEZA
limpio = raw.select(
    # "1.040.986" -> "1040986"
    F.regexp_replace(F.col("ID de caso"), r"\.", "").alias("id"),
    # "11.001" -> "11001"; "8.001" -> "08001" (5 dígitos DIVIPOLA)
    F.lpad(F.regexp_replace(F.col("Código DIVIPOLA municipio"), r"\.", ""), 5, "0").alias("cod_muni"),
    F.upper(F.trim(F.col("Nombre municipio"))).alias("municipio"),
    F.upper(F.trim(F.col("Nombre departamento"))).alias("departamento"),
    # Unidad de edad: 1 = años, 2 = meses, 3 = días. Meses y días -> 0 años.
    F.when(F.col("Unidad de medida de edad") == "1", F.col("Edad").cast("int"))
     .otherwise(F.lit(0)).alias("edad"),
    F.upper(F.trim(F.col("Sexo"))).alias("sexo"),                      # f/m -> F/M
    F.trim(F.col("Tipo de contagio")).alias("tipo_contagio"),
    F.initcap(F.lower(F.trim(F.col("Ubicación del caso")))).alias("ubicacion"),  # casa/CASA -> Casa
    F.initcap(F.lower(F.trim(F.col("Estado")))).alias("estado_raw"),             # leve -> Leve
    fecha("fecha reporte web").alias("fecha_reporte"),
    fecha("Fecha de notificación").alias("fecha_notificacion"),
    fecha("Fecha de muerte").alias("fecha_muerte"),
    F.initcap(F.lower(F.trim(F.col("Nombre del país")))).alias("pais_origen"),
)

# El código de departamento sale del código de municipio (en el CSV original
# Barranquilla trae "8.001" como departamento).
# Si hay fecha de muerte, el estado es Fallecido (hay ~4.144 casos sin Estado).
casos = (
    limpio
    .withColumn("cod_depto", F.substring(F.col("cod_muni"), 1, 2))
    .withColumn(
        "estado",
        F.when(F.col("fecha_muerte").isNotNull(), F.lit("Fallecido"))
         .when(F.col("estado_raw").isNull(), F.lit("Sin dato"))
         .otherwise(F.col("estado_raw")),
    )
    .filter(F.col("id").isNotNull() & (F.col("id") != "")
            & F.col("cod_muni").isNotNull() & F.col("fecha_notificacion").isNotNull())
    .dropDuplicates(["id"])
    .cache()
)
n_limpio = casos.count()
print(f"Registros tras limpieza: {n_limpio:,} (descartados: {n_raw - n_limpio:,})")

# Diagnóstico para el informe: casos cuyo estado original NO era Fallecido
# pero tienen fecha de muerte (si sale 0 o muy bajo, la regla es segura).
sobrescritos = casos.filter(
    (F.col("estado") == "Fallecido") & (F.col("estado_raw").isNull() | (F.col("estado_raw") != "Fallecido"))
).count()
print(f"Casos marcados Fallecido por tener fecha de muerte: {sobrescritos:,}")
t_limpieza = time.perf_counter() - t0

# ---------------------------------------------------------------- 3. PARQUET
casos.write.mode("overwrite").partitionBy("cod_depto").parquet(PARQUET)
print(f"Parquet escrito en {PARQUET} (particionado por cod_depto)")

# ---------------------------------------------------------------- 4. ARISTAS
# Dentro de cada municipio ordenamos los casos por fecha de notificación (no
# tiene nulos). Cada caso "Relacionado" toma como "padre" a uno de los K casos
# anteriores del mismo municipio. Resultado: un bosque (sin ciclos) donde se
# puede rastrear hacia atrás (¿de quién viene?) y hacia adelante (¿a quién llegó?).
w = Window.partitionBy("cod_muni").orderBy("fecha_notificacion", "id")
ordenados = (
    casos.select("id", "cod_muni", "tipo_contagio", "fecha_notificacion")
    .withColumn("pos", F.row_number().over(w))
)

hijos = (
    ordenados.filter((F.col("tipo_contagio") == "Relacionado") & (F.col("pos") > 1))
    .withColumn("n_cand", F.least(F.lit(K_VENTANA), F.col("pos") - 1))
    .withColumn("pos_padre", F.col("pos") - 1 - F.pmod(F.hash(F.col("id")), F.col("n_cand")))
)
padres = ordenados.select(
    F.col("cod_muni").alias("cod_muni_p"),
    F.col("pos").alias("pos_p"),
    F.col("id").alias("origen_id"),
    F.col("fecha_notificacion").alias("fecha_padre"),
)
aristas = hijos.join(
    padres,
    (F.col("cod_muni") == F.col("cod_muni_p")) & (F.col("pos_padre") == F.col("pos_p")),
).select(
    F.col("origen_id"),
    F.col("id").alias("destino_id"),
    F.datediff(F.col("fecha_notificacion"), F.col("fecha_padre")).alias("dias"),
)

# ---------------------------------------------------------------- 5. CSV PARA NEO4J
t1 = time.perf_counter()
exportar_csv(
    casos.select("id", "cod_muni", "municipio", "cod_depto", "departamento", "edad",
                 "sexo", "tipo_contagio", "estado", "ubicacion", "fecha_notificacion"),
    "nodos_casos",
)
exportar_csv(
    casos.groupBy("cod_muni").agg(
        F.first("municipio").alias("municipio"),
        F.first("cod_depto").alias("cod_depto"),
        F.first("departamento").alias("departamento"),
        F.count("*").alias("total_casos"),
    ),
    "nodos_municipios",
)
# Origen REAL de los casos importados (columna "Nombre del país")
exportar_csv(
    casos.filter(F.col("pais_origen").isNotNull()).select(
        F.col("pais_origen").alias("pais"), F.col("id").alias("caso_id")),
    "origen_importados",
)
exportar_csv(aristas, "aristas_contagio")
t_export = time.perf_counter() - t1

n_aristas = aristas.count()
print("\n===== RESUMEN (úsalo en el informe) =====")
print(f"Registros leídos:      {n_raw:,}")
print(f"Nodos (casos):         {n_limpio:,}")
print(f"Aristas simuladas:     {n_aristas:,}  (K_VENTANA={K_VENTANA})")
print(f"Tiempo limpieza:       {t_limpieza:.1f} s")
print(f"Tiempo exportación:    {t_export:.1f} s")
print(f"Tiempo total:          {time.perf_counter() - t0:.1f} s")
casos.groupBy("tipo_contagio").count().show()
casos.groupBy("estado").count().show()

spark.stop()
