# Orden de ejecución (parte técnica)

```
cp .env.example .env            # y cambiar la clave (guardar con saltos de línea LF)
pip install -r requirements.txt
docker compose up -d

# 1. Spark: limpia, genera Parquet y CSV de Neo4j
docker compose exec spark /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 4g /app/procesamiento/spark_etl_propagacion.py

# 2. Cassandra: esquema
docker compose exec -T cassandra cqlsh < modelos/cassandra_schema.cql

# 3. Kafka -> Cassandra (dos terminales)
python ingesta/kafka_producer.py              # LIMITE=None para los 639 097
python ingesta/kafka_consumer_cassandra.py

# 4. Neo4j (Neo4j Browser, localhost:7474): ejecutar en orden
#    consultas/neo4j/00_constraints.cypher, 01_carga.cypher, 10_trazabilidad.cypher, 20_gds.cypher

# 5. Demo CQL
docker compose exec -T cassandra cqlsh < consultas/cassandra/consultas.cql
```
