"""
Consumidor Kafka -> Cassandra (Reto 5, trazabilidad COVID-19 Colombia).

Lee los eventos del tópico 'covid-casos-stream' (los publica kafka_producer.py)
y los escribe en las dos tablas del keyspace covid_operational:
  - casos_por_municipio_mes  (consulta: casos de un municipio en un mes)
  - caso_por_id              (consulta: datos de un caso por su id)

Detalles de diseño:
  * Las escrituras son UPSERT (INSERT en Cassandra), así que reprocesar un
    mensaje no duplica nada: es idempotente.
  * Los offsets se confirman DESPUÉS de escribir en Cassandra (at-least-once).
  * Se escribe en lotes con execute_concurrent (no BATCH de Cassandra, que es
    un anti-patrón cuando las filas caen en particiones distintas).
  * Termina solo cuando no llegan mensajes durante --idle-exit segundos, para
    poder medir el rendimiento en la demo.

Ejecutar desde la raíz del proyecto (con los contenedores arriba y el esquema
ya aplicado, ver README):
    python ingesta/kafka_consumer_cassandra.py
    python ingesta/kafka_consumer_cassandra.py --idle-exit 20 --batch 1000
"""
import argparse
import datetime as dt
import json
import os
import time

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
# Python >= 3.12 ya no trae 'asyncore' y en Windows no hay libev: hay que
# indicar el modo de conexión asyncio de forma explícita.
from cassandra.io.asyncioreactor import AsyncioConnection
from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "covid-casos-stream")
CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
KEYSPACE = "covid_operational"

INS_POR_MUNI = (
    "INSERT INTO casos_por_municipio_mes "
    "(cod_muni, anio_mes, fecha_reporte, id_caso, departamento, municipio, "
    " edad, sexo, tipo_contagio, estado, ubicacion) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)
INS_POR_ID = (
    "INSERT INTO caso_por_id "
    "(id_caso, cod_muni, municipio, departamento, edad, sexo, tipo_contagio, "
    " estado, ubicacion, fecha_reporte, fecha_notificacion) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)


def a_fecha(texto):
    """'2020-10-28' -> datetime.date (o None)."""
    if not texto:
        return None
    try:
        return dt.date.fromisoformat(texto[:10])
    except ValueError:
        return None


def filas_desde_evento(ev):
    """Convierte un evento JSON en (fila_tabla_municipio, fila_tabla_id).
    Devuelve None si el evento no se puede guardar (sin id, sin municipio o
    sin ninguna fecha para la clave de clustering)."""
    id_caso = ev.get("id_caso")
    cod_muni = ev.get("cod_muni")
    f_notif = a_fecha(ev.get("fecha_notificacion"))
    # fecha_reporte es parte de la clave primaria y no puede ser nula:
    # si falta, se usa la fecha de notificación.
    f_rep = a_fecha(ev.get("fecha_reporte")) or f_notif
    if not id_caso or not cod_muni or f_rep is None:
        return None

    anio_mes = f_rep.strftime("%Y-%m")
    comunes = (
        ev.get("departamento"), ev.get("municipio"), ev.get("edad"),
        ev.get("sexo"), ev.get("tipo_contagio"), ev.get("estado"),
        ev.get("ubicacion"),
    )
    depto, muni, edad, sexo, tipo, estado, ubic = comunes
    fila_muni = (cod_muni, anio_mes, f_rep, id_caso, depto, muni,
                 edad, sexo, tipo, estado, ubic)
    fila_id = (id_caso, cod_muni, muni, depto, edad, sexo, tipo, estado,
               ubic, f_rep, f_notif)
    return fila_muni, fila_id


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--batch", type=int, default=500,
                    help="máx. de mensajes por ciclo de escritura")
    ap.add_argument("--idle-exit", type=int, default=15,
                    help="segundos sin mensajes para terminar (0 = no terminar)")
    ap.add_argument("--group", default="cassandra-writer", help="consumer group")
    ap.add_argument("--concurrency", type=int, default=64,
                    help="escrituras simultáneas hacia Cassandra")
    args = ap.parse_args()

    cluster = Cluster(CASSANDRA_HOSTS, connection_class=AsyncioConnection)
    sesion = cluster.connect(KEYSPACE)
    p_muni = sesion.prepare(INS_POR_MUNI)
    p_id = sesion.prepare(INS_POR_ID)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP],
        group_id=args.group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        max_poll_records=args.batch,
    )

    print(f"Consumiendo '{TOPIC}' -> Cassandra '{KEYSPACE}' (grupo '{args.group}')")
    leidos = escritos = descartados = errores = 0
    inicio = time.perf_counter()
    ultimo_mensaje = inicio

    try:
        while True:
            lote = consumer.poll(timeout_ms=1000, max_records=args.batch)
            mensajes = [m for msgs in lote.values() for m in msgs]
            if not mensajes:
                if args.idle_exit and time.perf_counter() - ultimo_mensaje > args.idle_exit:
                    print(f"Sin mensajes nuevos en {args.idle_exit} s: fin.")
                    break
                continue
            ultimo_mensaje = time.perf_counter()
            leidos += len(mensajes)

            filas_muni, filas_id = [], []
            for m in mensajes:
                par = filas_desde_evento(m.value)
                if par is None:
                    descartados += 1
                    continue
                filas_muni.append(par[0])
                filas_id.append(par[1])

            for prep, filas in ((p_muni, filas_muni), (p_id, filas_id)):
                resultados = execute_concurrent_with_args(
                    sesion, prep, filas, concurrency=args.concurrency,
                    raise_on_first_error=False)
                errores += sum(1 for ok, _ in resultados if not ok)
            escritos += len(filas_muni)

            consumer.commit()  # solo después de escribir
            if leidos % 5000 < args.batch:
                seg = time.perf_counter() - inicio
                print(f"  {leidos:,} leídos | {escritos:,} escritos | {leidos / seg:,.0f} msg/s")
    except KeyboardInterrupt:
        print("Interrumpido por el usuario.")
    finally:
        seg = max(time.perf_counter() - inicio - args.idle_exit, 1e-9)
        consumer.close()
        cluster.shutdown()

    print("\n===== RESUMEN (úsalo en el informe) =====")
    print(f"Mensajes leídos:        {leidos:,}")
    print(f"Eventos escritos:       {escritos:,} (en 2 tablas)")
    print(f"Descartados (sin clave): {descartados:,}")
    print(f"Errores de escritura:   {errores:,}")
    print(f"Tiempo aprox. activo:   {seg:.1f} s  (~{leidos / seg:,.0f} msg/s)")


if __name__ == "__main__":
    main()
