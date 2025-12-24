"""
================================================================================
TAREA 3 - PROCESAMIENTO EN TIEMPO REAL - SPARK STREAMING CONSUMER
================================================================================
Curso: Big Data - 202016911
Universidad Nacional Abierta y a Distancia (UNAD)

Descripción: 
    Script consumidor (consumer) de Kafka que utiliza Spark Streaming para
    procesar datos en tiempo real. Lee mensajes del topic 'sensor_data',
    realiza análisis en ventanas de tiempo y muestra estadísticas.

Autor: Sergio Mauricio Robayo Rojas - Código: 1032938094
Fecha: Abril 2026

Instrucciones de ejecución:
    1. Asegúrese de que Kafka esté corriendo
    2. Ejecute primero el producer: python3 kafka_producer.py
    3. En otra terminal ejecute este script:
       spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, avg, 
    max as spark_max, min as spark_min,
    current_timestamp, expr, lit
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, 
    FloatType, TimestampType, LongType
)
import logging

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "sensor_data"


# ==============================================================================
# ESQUEMA DE DATOS
# ==============================================================================
# Definir el esquema de los datos JSON que recibimos
sensor_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("timestamp", LongType(), True),
    StructField("datetime", StringType(), True),
    StructField("departamento", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("tipo_evento", StringType(), True),
    StructField("zona", StringType(), True),
    StructField("latitud", FloatType(), True),
    StructField("longitud", FloatType(), True),
    StructField("nivel_riesgo", StringType(), True),
    StructField("temperatura", FloatType(), True),
    StructField("humedad", FloatType(), True)
])


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================
def main():
    """
    Función principal del consumidor de Spark Streaming.
    """
    print("\n" + "=" * 70)
    print("SPARK STREAMING CONSUMER - ANÁLISIS EN TIEMPO REAL")
    print("=" * 70)
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print("=" * 70 + "\n")
    
    # =========================================================================
    # CREAR SESIÓN DE SPARK
    # =========================================================================
    spark = SparkSession.builder \
        .appName("BigData_Tarea3_SparkStreaming") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .getOrCreate()
    
    # Reducir verbosidad de logs
    spark.sparkContext.setLogLevel("WARN")
    
    print("✓ Sesión de Spark iniciada")
    print(f"  - Versión: {spark.version}")
    print(f"  - App: {spark.sparkContext.appName}")
    
    # =========================================================================
    # LEER STREAM DE KAFKA
    # =========================================================================
    print("\n[1] Conectando a Kafka...")
    
    df_raw = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("✓ Conectado a Kafka exitosamente")
    
    # =========================================================================
    # PARSEAR DATOS JSON
    # =========================================================================
    print("[2] Configurando parser de datos...")
    
    df_parsed = df_raw \
        .select(
            col("key").cast("string").alias("event_key"),
            from_json(col("value").cast("string"), sensor_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select(
            "event_key",
            "kafka_timestamp",
            "data.*"
        )
    
    print("✓ Parser configurado")
    
    # =========================================================================
    # ANÁLISIS 1: CONTEO DE EVENTOS POR TIPO EN VENTANAS DE 1 MINUTO
    # =========================================================================
    print("[3] Configurando análisis por ventana de tiempo...")
    
    events_by_type = df_parsed \
        .withWatermark("kafka_timestamp", "2 minutes") \
        .groupBy(
            window(col("kafka_timestamp"), "1 minute"),
            "tipo_evento"
        ) \
        .agg(
            count("*").alias("total_eventos"),
            avg("temperatura").alias("temp_promedio"),
            avg("humedad").alias("humedad_promedio")
        )
    
    # =========================================================================
    # ANÁLISIS 2: ESTADÍSTICAS POR DEPARTAMENTO
    # =========================================================================
    stats_by_dept = df_parsed \
        .withWatermark("kafka_timestamp", "2 minutes") \
        .groupBy(
            window(col("kafka_timestamp"), "1 minute"),
            "departamento"
        ) \
        .agg(
            count("*").alias("total_eventos"),
            avg("temperatura").alias("temp_promedio")
        )
    
    # =========================================================================
    # ANÁLISIS 3: ALERTAS DE RIESGO ALTO
    # =========================================================================
    high_risk_alerts = df_parsed \
        .filter(col("nivel_riesgo").isin(["ALTO", "CRITICO"]))
    
    # =========================================================================
    # INICIAR QUERIES DE STREAMING
    # =========================================================================
    print("[4] Iniciando queries de streaming...")
    print("-" * 70)
    print("Mostrando estadísticas en tiempo real...")
    print("(Ctrl+C para detener)")
    print("=" * 70 + "\n")
    
    # Query 1: Eventos por tipo (ventana de 1 minuto)
    query_events = events_by_type \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="30 seconds") \
        .queryName("eventos_por_tipo") \
        .start()
    
    # Query 2: Estadísticas por departamento
    query_dept = stats_by_dept \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="30 seconds") \
        .queryName("stats_departamento") \
        .start()
    
    # Query 3: Alertas de alto riesgo (append mode)
    query_alerts = high_risk_alerts \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .queryName("alertas_riesgo") \
        .start()
    
    print("✓ Todas las queries iniciadas")
    print("\n" + "=" * 70)
    print("RESULTADOS EN TIEMPO REAL")
    print("=" * 70 + "\n")
    
    # Esperar a que terminen las queries
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n" + "-" * 70)
        print("✓ Streaming detenido por el usuario")
        print("-" * 70)
    finally:
        # Detener todas las queries
        for query in spark.streams.active:
            query.stop()
        spark.stop()
        print("✓ Sesión de Spark cerrada")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================
if __name__ == "__main__":
    main()
