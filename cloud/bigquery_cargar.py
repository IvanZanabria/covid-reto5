"""
Carga los datos analíticos a BigQuery (Reto 5).

Tablas del dataset 'covid_trazabilidad':
  casos             <- data/processed/bq/casos.parquet   (lo genera spark_analitica.py)
  aristas_contagio  <- data/processed/aristas_contagio.csv (lo genera spark_etl_propagacion.py)
  nodos_criticos    <- data/processed/nodos_criticos.csv  (OPCIONAL: exportado de Neo4j, ver README de consultas)

Diseño de 'casos':
  * Clustering por (fecha_notificacion, cod_depto, cod_muni): ordena físicamente
    los datos, así filtrar por rango de fechas, departamento o municipio lee
    menos bloques (y menos bytes facturados).
  * No se particiona por fecha a propósito: el sandbox gratuito de BigQuery hace
    expirar las particiones a los 60 días y los datos (2020-2023) se perderían.
    Con facturación activa lo ideal sería particionar por MES de fecha_notificacion.

Autenticación (NUNCA subir llaves a Git). Una de dos:
  a) gcloud auth application-default login
  b) GOOGLE_APPLICATION_CREDENTIALS=<ruta al JSON de la cuenta de servicio>
Variable obligatoria: GCP_PROJECT (id del proyecto de Google Cloud).

Uso (desde la raíz del proyecto, con el venv activo):
  python cloud/bigquery_cargar.py
"""
import os
import sys
import time
from pathlib import Path

from google.cloud import bigquery

DATASET = "covid_trazabilidad"
CASOS = Path("data/processed/bq/casos.parquet")
ARISTAS = Path("data/processed/aristas_contagio.csv")
NODOS_CRITICOS = Path("data/processed/nodos_criticos.csv")


def cargar_env(ruta=".env"):
    """Lee el archivo .env (clave=valor) y lo pone en el entorno.
    Python no lo hace solo; Docker sí. Lo que ya esté definido en el entorno
    tiene prioridad. Tolera saltos de línea de Windows (CRLF)."""
    if not os.path.exists(ruta):
        return
    with open(ruta, encoding="utf-8") as f:
        for linea in f:
            linea = linea.strip()
            if linea and not linea.startswith("#") and "=" in linea:
                clave, valor = linea.split("=", 1)
                os.environ.setdefault(clave.strip(), valor.strip().strip('"').strip("'"))


def main():
    cargar_env()
    proyecto = os.getenv("GCP_PROJECT")
    if not proyecto:
        sys.exit("Falta la variable de entorno GCP_PROJECT (ver .env.example).")
    if not CASOS.exists():
        sys.exit(f"No existe {CASOS}. Ejecuten primero procesamiento/spark_analitica.py")

    cliente = bigquery.Client(project=proyecto)
    ds = bigquery.Dataset(f"{proyecto}.{DATASET}")
    ds.location = os.getenv("BQ_LOCATION", "US")
    cliente.create_dataset(ds, exists_ok=True)

    def cargar(archivo, tabla, config):
        t0 = time.perf_counter()
        # Si la tabla ya existe con otra partición/clustering, BigQuery rechaza
        # reemplazarla. Como la carga es completa (TRUNCATE), se borra y se recrea.
        cliente.delete_table(f"{proyecto}.{DATASET}.{tabla}", not_found_ok=True)
        with open(archivo, "rb") as f:
            job = cliente.load_table_from_file(f, f"{proyecto}.{DATASET}.{tabla}", job_config=config)
        job.result()
        # output_rows es el dato del propio trabajo de carga: es confiable al instante.
        # (table.num_rows puede tardar en actualizarse en tablas particionadas.)
        filas = job.output_rows
        print(f"  {tabla}: {filas:,} filas en {time.perf_counter() - t0:.1f} s")

    print(f"Cargando en {proyecto}.{DATASET} ...")

    cargar(CASOS, "casos", bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # Sin partición por fecha: en el sandbox de BigQuery las particiones
        # expiran a los 60 días y los datos de 2020-2023 se borrarían al cargarlos.
        clustering_fields=["fecha_notificacion", "cod_depto", "cod_muni"],
    ))

    if ARISTAS.exists():
        cargar(ARISTAS, "aristas_contagio", bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("origen_id", "STRING"),
                bigquery.SchemaField("destino_id", "STRING"),
                bigquery.SchemaField("dias", "INT64"),
            ],
            clustering_fields=["origen_id"],
        ))
    else:
        print(f"  (se omite aristas_contagio: no existe {ARISTAS})")

    if NODOS_CRITICOS.exists():
        cargar(NODOS_CRITICOS, "nodos_criticos", bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("caso", "STRING"),
                bigquery.SchemaField("contagios_directos", "INT64"),
            ],
        ))
    else:
        print(f"  (se omite nodos_criticos: no existe {NODOS_CRITICOS}, es opcional)")

    print("Listo.")


if __name__ == "__main__":
    main()
