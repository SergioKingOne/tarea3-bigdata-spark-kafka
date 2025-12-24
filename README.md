# Tarea 3 - Procesamiento de Datos con Apache Spark

## Universidad Nacional Abierta y a Distancia (UNAD)
### Curso: Big Data

---

## Descripcion del Proyecto

Este repositorio contiene la implementacion de la Tarea 3 del curso de Big Data, que consiste en el procesamiento de datos utilizando Apache Spark tanto en modo **batch** como en **tiempo real (streaming)** con Apache Kafka.

### Objetivo General
Disenar e implementar soluciones de almacenamiento y procesamiento de grandes volumenes de datos utilizando herramientas como Hadoop, Spark y Kafka.

### Dataset Utilizado
- **Nombre**: Hurto a Personas en Colombia
- **Fuente**: [Datos Abiertos Colombia](https://www.datos.gov.co)
- **Descripcion**: Dataset con informacion detallada sobre incidentes de hurto a personas registrados a nivel nacional.

---

## Estructura del Repositorio

```
tarea3/
|
├── README.md                      # Este archivo
├── spark_batch_analysis.py        # Procesamiento batch con PySpark
├── kafka_producer.py              # Productor de datos para Kafka
├── spark_streaming_consumer.py    # Consumidor Spark Streaming
├── requirements.txt               # Dependencias de Python
└── visualizaciones/               # Graficos generados por el analisis
    ├── 01_top_departamentos.png
    ├── 02_distribucion_genero.png
    ├── 03_tipo_arma.png
    └── 04_franja_horaria.png
```

---

## Requisitos Previos

### Software Necesario
- Python 3.8+
- Apache Spark 3.5.x
- Apache Kafka 3.x
- Java 11+

### Instalacion de Dependencias
```bash
pip install -r requirements.txt
```

---

## Instrucciones de Ejecucion

### 1. Procesamiento Batch (Spark)

```bash
# Ejecutar el analisis batch
spark-submit spark_batch_analysis.py

# O directamente con Python
python3 spark_batch_analysis.py
```

**Resultados esperados:**
- Analisis exploratorio de datos (EDA)
- Estadisticas por departamento, genero, tipo de arma, etc.
- Visualizaciones guardadas en `/visualizaciones`
- Demostracion de operaciones RDD

### 2. Procesamiento en Tiempo Real (Kafka + Spark Streaming)

**Terminal 1 - Iniciar ZooKeeper:**
```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
```

**Terminal 2 - Iniciar Kafka:**
```bash
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
```

**Terminal 3 - Crear Topic:**
```bash
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
    --replication-factor 1 --partitions 1 --topic sensor_data
```

**Terminal 4 - Ejecutar Producer:**
```bash
python3 kafka_producer.py
```

**Terminal 5 - Ejecutar Consumer:**
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    spark_streaming_consumer.py
```

---

## Analisis Realizados

### Procesamiento Batch
1. **Estadisticas generales** del dataset
2. **Top 10 departamentos** con mas hurtos
3. **Distribucion por genero** de las victimas
4. **Distribucion por grupo etario**
5. **Analisis por tipo de arma/medio** utilizado
6. **Distribucion por zona** (urbana/rural)
7. **Analisis por franja horaria**
8. **Distribucion por dia de la semana**
9. **Top 10 municipios** con mas hurtos
10. **Analisis cruzado**: Genero vs Tipo de Arma

### Procesamiento Streaming
1. **Conteo de eventos** por tipo en ventanas de 1 minuto
2. **Estadisticas por departamento** en tiempo real
3. **Alertas de riesgo alto/critico**
4. **Promedios de temperatura y humedad** por sensor

---

## Resultados y Visualizaciones

Las visualizaciones generadas se encuentran en la carpeta `visualizaciones/`:

| Archivo | Descripcion |
|---------|-------------|
| `01_top_departamentos.png` | Top 10 departamentos con mayor cantidad de hurtos |
| `02_distribucion_genero.png` | Distribucion porcentual por genero de la victima |
| `03_tipo_arma.png` | Distribucion por tipo de arma/medio utilizado |
| `04_franja_horaria.png` | Distribucion por franja horaria del dia |

---

## Tecnologias Utilizadas

| Tecnologia | Version | Uso |
|------------|---------|-----|
| Apache Spark | 3.5.3 | Procesamiento batch y streaming |
| Apache Kafka | 3.8.0 | Mensajeria en tiempo real |
| Python | 3.10+ | Lenguaje de programacion |
| PySpark | 3.5.3 | API de Python para Spark |
| Matplotlib | 3.8+ | Visualizaciones |
| Pandas | 2.0+ | Manipulacion de datos |

---

## Video Demostrativo

[Ver video en YouTube](https://youtu.be/XRBzGOP4UeI)

---

## Referencias

- Apache Spark Documentation: https://spark.apache.org/docs/latest/
- Apache Kafka Documentation: https://kafka.apache.org/documentation/
- Datos Abiertos Colombia: https://www.datos.gov.co

---

## Licencia

Este proyecto fue desarrollado con fines academicos para la Universidad Nacional Abierta y a Distancia (UNAD).

