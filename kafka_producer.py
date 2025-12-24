"""
================================================================================
TAREA 3 - PROCESAMIENTO EN TIEMPO REAL - KAFKA PRODUCER
================================================================================
Curso: Big Data - 202016911
Universidad Nacional Abierta y a Distancia (UNAD)

Descripción: 
    Script productor (producer) de Kafka que genera datos simulados de 
    sensores de seguridad y los envía al topic 'sensor_data'.
    
    Este script simula la generación de datos en tiempo real que podrían
    provenir de sensores de seguridad ciudadana.

Autor: Sergio Mauricio Robayo Rojas - Código: 1032938094
Fecha: Abril 2026

Instrucciones de ejecución:
    1. Asegúrese de que Kafka esté corriendo
    2. Ejecute: python3 kafka_producer.py
================================================================================
"""

import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'sensor_data'

# Datos para generación aleatoria
DEPARTAMENTOS = [
    "CUNDINAMARCA", "ANTIOQUIA", "VALLE DEL CAUCA", 
    "ATLÁNTICO", "SANTANDER", "BOLIVAR"
]

MUNICIPIOS = {
    "CUNDINAMARCA": ["BOGOTÁ D.C.", "SOACHA", "FACATATIVÁ"],
    "ANTIOQUIA": ["MEDELLÍN", "BELLO", "ENVIGADO"],
    "VALLE DEL CAUCA": ["CALI", "PALMIRA", "BUENAVENTURA"],
    "ATLÁNTICO": ["BARRANQUILLA", "SOLEDAD", "MALAMBO"],
    "SANTANDER": ["BUCARAMANGA", "FLORIDABLANCA", "GIRÓN"],
    "BOLIVAR": ["CARTAGENA", "MAGANGUÉ", "TURBACO"]
}

TIPOS_EVENTO = [
    "HURTO_PERSONA", "HURTO_VEHICULO", "HURTO_COMERCIO",
    "ALERTA_SEGURIDAD", "EMERGENCIA_MEDICA"
]

ZONAS = ["URBANA", "RURAL"]


# ==============================================================================
# GENERADOR DE DATOS
# ==============================================================================
def generate_sensor_data():
    """
    Genera datos simulados de un sensor de seguridad ciudadana.
    
    Returns:
        dict: Diccionario con los datos del evento
    """
    departamento = random.choice(DEPARTAMENTOS)
    
    return {
        "sensor_id": random.randint(1, 100),
        "timestamp": int(time.time()),
        "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "departamento": departamento,
        "municipio": random.choice(MUNICIPIOS[departamento]),
        "tipo_evento": random.choice(TIPOS_EVENTO),
        "zona": random.choice(ZONAS),
        "latitud": round(random.uniform(1.0, 12.0), 6),
        "longitud": round(random.uniform(-79.0, -67.0), 6),
        "nivel_riesgo": random.choice(["BAJO", "MEDIO", "ALTO", "CRITICO"]),
        "temperatura": round(random.uniform(15, 35), 2),
        "humedad": round(random.uniform(40, 90), 2)
    }


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================
def main():
    """
    Función principal del productor de Kafka.
    Genera y envía datos continuamente al topic de Kafka.
    """
    print("\n" + "=" * 70)
    print("KAFKA PRODUCER - DATOS DE SEGURIDAD EN TIEMPO REAL")
    print("=" * 70)
    print(f"Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print("-" * 70)
    print("Iniciando producción de mensajes... (Ctrl+C para detener)")
    print("=" * 70 + "\n")
    
    # Crear productor de Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8') if x else None
    )
    
    message_count = 0
    
    try:
        while True:
            # Generar datos del sensor
            sensor_data = generate_sensor_data()
            
            # Usar el tipo de evento como clave para particionamiento
            key = sensor_data['tipo_evento']
            
            # Enviar mensaje a Kafka
            producer.send(KAFKA_TOPIC, key=key, value=sensor_data)
            
            message_count += 1
            
            # Mostrar mensaje enviado
            print(f"[{message_count}] Sent: Sensor {sensor_data['sensor_id']:3d} | "
                  f"{sensor_data['tipo_evento']:20s} | "
                  f"{sensor_data['municipio']:15s} | "
                  f"Riesgo: {sensor_data['nivel_riesgo']:8s} | "
                  f"{sensor_data['datetime']}")
            
            # Esperar 1 segundo entre mensajes
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n" + "-" * 70)
        print(f"✓ Producción detenida. Total de mensajes enviados: {message_count:,}")
        print("-" * 70)
    finally:
        # Asegurar que todos los mensajes se envíen
        producer.flush()
        producer.close()
        print("✓ Productor cerrado correctamente")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================
if __name__ == "__main__":
    main()
