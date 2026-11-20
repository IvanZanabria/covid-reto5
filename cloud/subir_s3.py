"""
Sube el data lake a Amazon S3 (Reto 5).

Organización del bucket (zonas del data lake):
  s3://<bucket>/raw/casos_covid_600k.csv        <- dato crudo original, sin tocar
  s3://<bucket>/curated/casos_parquet/cod_depto=XX/part-*.parquet
                                                <- salida limpia de Spark,
                                                   Parquet particionado por departamento

Por qué Parquet particionado en 'curated': formato columnar comprimido (mucho
menos tamaño y lectura que el CSV) y la partición por departamento permite que
Spark lea solo los departamentos que necesita.

Credenciales: SOLO por variables de entorno (nunca en el código ni en Git):
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET

Uso (desde la raíz del proyecto, con el venv activo):
  python cloud/subir_s3.py
"""
import os
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

RAW_LOCAL = Path("data/raw/casos_covid_600k.csv")
PARQUET_LOCAL = Path("data/processed/casos_parquet")


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
    bucket = os.getenv("S3_BUCKET")
    region = os.getenv("AWS_REGION", "us-east-1")
    if not bucket:
        sys.exit("Falta la variable de entorno S3_BUCKET (ver .env.example).")

    s3 = boto3.client("s3", region_name=region)

    # Crear el bucket si no existe (us-east-1 no admite LocationConstraint).
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            args = {"Bucket": bucket}
            if region != "us-east-1":
                args["CreateBucketConfiguration"] = {"LocationConstraint": region}
            s3.create_bucket(**args)
            print(f"Bucket '{bucket}' creado en {region}")
        else:
            sys.exit(f"No se puede acceder al bucket '{bucket}': {e}")
    except NoCredentialsError:
        sys.exit("No hay credenciales de AWS (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY).")

    archivos = []
    if RAW_LOCAL.exists():
        archivos.append((RAW_LOCAL, f"raw/{RAW_LOCAL.name}"))
    for p in sorted(PARQUET_LOCAL.rglob("*.parquet")):
        archivos.append((p, "curated/casos_parquet/" + p.relative_to(PARQUET_LOCAL).as_posix()))
    if not archivos:
        sys.exit("No hay archivos para subir. ¿Ejecutaron antes el ETL de Spark?")

    total_mb = sum(f.stat().st_size for f, _ in archivos) / 1e6
    print(f"Subiendo {len(archivos)} archivos ({total_mb:,.1f} MB) a s3://{bucket}/ ...")
    t0 = time.perf_counter()
    for i, (local, clave) in enumerate(archivos, start=1):
        s3.upload_file(str(local), bucket, clave)   # multipart automático si es grande
        print(f"  [{i}/{len(archivos)}] s3://{bucket}/{clave}")
    seg = time.perf_counter() - t0
    print(f"\nListo en {seg:.1f} s ({total_mb / seg:,.1f} MB/s)")


if __name__ == "__main__":
    main()
