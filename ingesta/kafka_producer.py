"""
Productor Kafka: envía los casos limpios al tópico 'covid-casos-stream'.

La limpieza es la MISMA que hace el ETL de Spark (códigos sin puntos, edad en
años, mayúsculas normalizadas), para que Kafka/Cassandra y Neo4j hablen igual.
"""
import json
import time

import pandas as pd
from kafka import KafkaProducer

BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "covid-casos-stream"
DATA_PATH = "data/raw/casos_covid_600k.csv"

LIMITE = None   # nº de eventos a enviar; None = los 639.097 (tarda mucho si PAUSA > 0)
PAUSA = 0.0     # segundos entre eventos. 0.01 simula "tiempo real" (~1 h 45 min con todo)


def txt(v):
    """NaN -> None; si no, texto sin espacios."""
    return None if pd.isna(v) else str(v).strip()


def fecha(v):
    v = txt(v)
    return v[:10] if v else None          # '2020-10-28 00:00:00' -> '2020-10-28'


def construir_evento(row):
    cod_muni = (txt(row["Código DIVIPOLA municipio"]) or "").replace(".", "").zfill(5)
    unidad = txt(row["Unidad de medida de edad"])
    try:
        edad = int(txt(row["Edad"])) if unidad == "1" else 0   # meses/días -> 0 años
    except (TypeError, ValueError):
        edad = None

    fecha_muerte = fecha(row["Fecha de muerte"])
    estado = txt(row["Estado"])
    if fecha_muerte:
        estado = "Fallecido"
    elif estado:
        estado = estado.title()
    else:
        estado = "Sin dato"

    ubicacion = txt(row["Ubicación del caso"])
    pais = txt(row["Nombre del país"])
    return {
        "id_caso": (txt(row["ID de caso"]) or "").replace(".", ""),
        "fecha_reporte": fecha(row["fecha reporte web"]),
        "fecha_notificacion": fecha(row["Fecha de notificación"]),
        "cod_depto": cod_muni[:2],
        "departamento": (txt(row["Nombre departamento"]) or "").upper(),
        "cod_muni": cod_muni,
        "municipio": (txt(row["Nombre municipio"]) or "").upper(),
        "edad": edad,
        "sexo": (txt(row["Sexo"]) or "").upper(),
        "tipo_contagio": txt(row["Tipo de contagio"]),
        "ubicacion": ubicacion.title() if ubicacion else None,
        "estado": estado,
        "pais_origen": pais.title() if pais else None,
    }


# dtype=str: evita que pandas convierta "11.001" en el número 11.001
df = pd.read_csv(DATA_PATH, dtype=str)
if LIMITE:
    df = df.head(LIMITE)

# La clave del mensaje es el municipio: los eventos de un mismo municipio caen
# en la misma partición de Kafka y conservan su orden.
producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP],
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)

print(f"Enviando {len(df):,} eventos a '{TOPIC_NAME}'...")
inicio = time.perf_counter()
for i, row in enumerate(df.to_dict("records"), start=1):
    evento = construir_evento(row)
    producer.send(TOPIC_NAME, key=evento["cod_muni"], value=evento)
    if i % 1000 == 0:
        print(f"  {i:,} enviados")
    if PAUSA:
        time.sleep(PAUSA)

producer.flush()
seg = time.perf_counter() - inicio
print(f"Listo: {len(df):,} eventos en {seg:.1f} s ({len(df) / seg:,.0f} eventos/s)")
