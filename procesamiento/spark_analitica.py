"""
Análisis masivo con Spark sobre el histórico (Reto 5).

Lee el Parquet curado (del disco local o de S3), calcula indicadores de
propagación y deja UN archivo Parquet limpio listo para cargar en BigQuery.

Ejecutar desde la raíz del proyecto, con los contenedores arriba.

  # Fuente local (no necesita AWS):
  docker compose exec spark /opt/spark/bin/spark-submit --master "local[*]" \
      --driver-memory 4g /app/procesamiento/spark_analitica.py --fuente local

  # Fuente S3 (necesita AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET):
  docker compose exec spark /opt/spark/bin/spark-submit --master "local[*]" \
      --driver-memory 4g \
      --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
      /app/procesamiento/spark_analitica.py --fuente s3
"""
import argparse
import glob
import os
import shutil
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--fuente", choices=["local", "s3"], default="local")
args = ap.parse_args()

SALIDA_BQ = "data/processed/bq"
builder = (
    SparkSession.builder.appName("CovidAnalitica")
    .config("spark.sql.shuffle.partitions", "16")
    # Sin esto, la carpeta cod_depto=08 se lee como número 8 y se pierde el cero.
    .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
)
if args.fuente == "s3":
    bucket = os.environ["S3_BUCKET"]
    region = os.getenv("AWS_REGION", "us-east-1")
    builder = (
        builder
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    )
    ENTRADA = f"s3a://{bucket}/curated/casos_parquet"
else:
    ENTRADA = "data/processed/casos_parquet"

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------- LECTURA
t0 = time.perf_counter()
casos = spark.read.parquet(ENTRADA).cache()
n = casos.count()
t_lectura = time.perf_counter() - t0
print(f"Fuente: {args.fuente} ({ENTRADA})")
print(f"Registros leídos: {n:,} en {t_lectura:.1f} s")

# ---------------------------------------------------------------- INDICADORES
t1 = time.perf_counter()

# Tiempo de detección = días entre que se notifica el caso y se reporta en la web.
con_det = casos.withColumn(
    "dias_deteccion", F.datediff(F.col("fecha_reporte"), F.col("fecha_notificacion"))
)

print("\n--- Casos por estado")
con_det.groupBy("estado").count().orderBy(F.desc("count")).show()

print("--- Top 10 departamentos por casos")
(con_det.groupBy("departamento").agg(F.count("*").alias("casos"))
    .orderBy(F.desc("casos")).show(10, truncate=False))

print("--- Tiempo de detección (días) por tipo de contagio")
(con_det.groupBy("tipo_contagio").agg(
    F.count("*").alias("casos"),
    F.round(F.avg("dias_deteccion"), 2).alias("dias_promedio"),
    F.expr("percentile_approx(dias_deteccion, 0.5)").alias("mediana"),
).orderBy(F.desc("casos")).show(truncate=False))

print("--- Casos por mes (serie de tiempo)")
(con_det.withColumn("mes", F.date_format("fecha_notificacion", "yyyy-MM"))
    .groupBy("mes").count().orderBy("mes").show(40))
t_indicadores = time.perf_counter() - t1

# ---------------------------------------------------------------- SALIDA PARA BIGQUERY
t2 = time.perf_counter()
os.makedirs(SALIDA_BQ, exist_ok=True)
tmp = f"{SALIDA_BQ}/_tmp_casos"
(con_det.select(
    F.col("id").alias("id_caso"), "cod_muni", "municipio", "cod_depto", "departamento",
    "edad", "sexo", "tipo_contagio", "estado", "ubicacion",
    "fecha_notificacion", "fecha_reporte", "dias_deteccion", "pais_origen")
    .coalesce(1).write.mode("overwrite").parquet(tmp))
parte = glob.glob(f"{tmp}/part-*.parquet")[0]
os.replace(parte, f"{SALIDA_BQ}/casos.parquet")
shutil.rmtree(tmp)
t_salida = time.perf_counter() - t2

print("\n===== RESUMEN (úsalo en el informe) =====")
print(f"Fuente:                 {args.fuente}")
print(f"Registros procesados:   {n:,}")
print(f"Tiempo de lectura:      {t_lectura:.1f} s")
print(f"Tiempo de indicadores:  {t_indicadores:.1f} s")
print(f"Tiempo de salida a BQ:  {t_salida:.1f} s")
print(f"Archivo para BigQuery:  {SALIDA_BQ}/casos.parquet")
spark.stop()
