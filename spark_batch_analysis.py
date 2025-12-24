"""
================================================================================
TAREA 3 - PROCESAMIENTO DE DATOS CON APACHE SPARK
================================================================================
Curso: Big Data - 202016911
Universidad Nacional Abierta y a Distancia (UNAD)

Descripción: 
    Script de procesamiento batch para análisis exploratorio de datos (EDA)
    del dataset de Hurto a Personas en Colombia.

Dataset: 
    Hurto a Personas - Datos Abiertos Colombia (datos.gov.co)
    
Autor: Sergio Mauricio Robayo Rojas - Código: 1032938094
Fecha: Abril 2026
================================================================================
"""

# ==============================================================================
# IMPORTACIÓN DE LIBRERÍAS
# ==============================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, max, min, sum as spark_sum,
    year, month, dayofweek, hour,
    when, lit, desc, asc,
    round as spark_round,
    to_date, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, DateType, TimestampType
)
import matplotlib.pyplot as plt
import pandas as pd
import os

# ==============================================================================
# CONFIGURACIÓN DE SPARK
# ==============================================================================
def create_spark_session():
    """
    Crea y configura una sesión de Spark para procesamiento local.
    
    Returns:
        SparkSession: Sesión de Spark configurada
    """
    spark = SparkSession.builder \
        .appName("BigData_Tarea3_HurtoPersonas_Batch") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    # Configurar nivel de log para reducir verbosidad
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 70)
    print("SESIÓN DE SPARK INICIADA CORRECTAMENTE")
    print("=" * 70)
    print(f"Versión de Spark: {spark.version}")
    print(f"App Name: {spark.sparkContext.appName}")
    print(f"Master: {spark.sparkContext.master}")
    print("=" * 70)
    
    return spark


# ==============================================================================
# CARGA DE DATOS
# ==============================================================================
def load_data(spark, file_path):
    """
    Carga el dataset de hurto a personas desde un archivo CSV.
    
    Args:
        spark: SparkSession activa
        file_path: Ruta al archivo CSV
        
    Returns:
        DataFrame: Dataset cargado en un DataFrame de Spark
    """
    print("\n[1] CARGANDO DATOS...")
    print("-" * 50)
    
    # Definir esquema para mejor rendimiento
    schema = StructType([
        StructField("DEPARTAMENTO", StringType(), True),
        StructField("MUNICIPIO", StringType(), True),
        StructField("FECHA_HECHO", StringType(), True),
        StructField("HORA_HECHO", StringType(), True),
        StructField("GENERO", StringType(), True),
        StructField("GRUPO_ETARIO", StringType(), True),
        StructField("ARMA_MEDIO", StringType(), True),
        StructField("ZONA", StringType(), True),
        StructField("CANTIDAD", IntegerType(), True)
    ])
    
    # Cargar datos
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(file_path)
    
    print(f"✓ Archivo cargado: {file_path}")
    print(f"✓ Total de registros: {df.count():,}")
    print(f"✓ Total de columnas: {len(df.columns)}")
    
    return df


def generate_sample_data(spark):
    """
    Genera datos de ejemplo para demostración si no hay archivo disponible.
    
    Args:
        spark: SparkSession activa
        
    Returns:
        DataFrame: Dataset de ejemplo
    """
    print("\n[1] GENERANDO DATOS DE EJEMPLO...")
    print("-" * 50)
    
    # Crear datos de ejemplo representativos
    data = [
        ("CUNDINAMARCA", "BOGOTÁ D.C.", "2024-01-15", "14:30", "MASCULINO", "ADULTOS", "ARMA BLANCA", "URBANA", 1),
        ("ANTIOQUIA", "MEDELLÍN", "2024-01-16", "18:45", "FEMENINO", "ADULTOS", "SIN EMPLEO DE ARMAS", "URBANA", 1),
        ("VALLE DEL CAUCA", "CALI", "2024-01-17", "09:15", "MASCULINO", "JOVENES", "ARMA DE FUEGO", "URBANA", 1),
        ("ATLÁNTICO", "BARRANQUILLA", "2024-01-18", "22:00", "MASCULINO", "ADULTOS", "ARMA BLANCA", "URBANA", 1),
        ("SANTANDER", "BUCARAMANGA", "2024-01-19", "11:30", "FEMENINO", "ADULTOS", "SIN EMPLEO DE ARMAS", "URBANA", 1),
        ("CUNDINAMARCA", "BOGOTÁ D.C.", "2024-01-20", "16:00", "MASCULINO", "ADOLESCENTES", "SIN EMPLEO DE ARMAS", "URBANA", 1),
        ("ANTIOQUIA", "MEDELLÍN", "2024-01-21", "08:00", "FEMENINO", "JOVENES", "ARMA BLANCA", "URBANA", 1),
        ("VALLE DEL CAUCA", "CALI", "2024-01-22", "20:30", "MASCULINO", "ADULTOS", "ARMA DE FUEGO", "URBANA", 1),
        ("CUNDINAMARCA", "BOGOTÁ D.C.", "2024-01-23", "13:45", "MASCULINO", "ADULTOS MAYORES", "SIN EMPLEO DE ARMAS", "RURAL", 1),
        ("BOYACÁ", "TUNJA", "2024-01-24", "10:00", "FEMENINO", "ADULTOS", "ARMA BLANCA", "URBANA", 1),
        ("CUNDINAMARCA", "BOGOTÁ D.C.", "2024-02-01", "15:30", "MASCULINO", "ADULTOS", "SIN EMPLEO DE ARMAS", "URBANA", 1),
        ("ANTIOQUIA", "MEDELLÍN", "2024-02-02", "19:00", "MASCULINO", "JOVENES", "ARMA BLANCA", "URBANA", 1),
        ("CUNDINAMARCA", "BOGOTÁ D.C.", "2024-02-03", "07:30", "FEMENINO", "ADULTOS", "SIN EMPLEO DE ARMAS", "URBANA", 1),
        ("VALLE DEL CAUCA", "CALI", "2024-02-04", "21:15", "MASCULINO", "ADULTOS", "ARMA DE FUEGO", "URBANA", 1),
        ("CUNDINAMARCA", "BOGOTÁ D.C.", "2024-02-05", "12:00", "FEMENINO", "ADOLESCENTES", "SIN EMPLEO DE ARMAS", "URBANA", 1),
        ("NARIÑO", "PASTO", "2024-02-06", "17:45", "MASCULINO", "ADULTOS", "ARMA BLANCA", "URBANA", 1),
        ("CUNDINAMARCA", "BOGOTÁ D.C.", "2024-02-07", "23:00", "MASCULINO", "JOVENES", "ARMA BLANCA", "URBANA", 1),
        ("RISARALDA", "PEREIRA", "2024-02-08", "14:00", "FEMENINO", "ADULTOS", "SIN EMPLEO DE ARMAS", "URBANA", 1),
        ("CUNDINAMARCA", "BOGOTÁ D.C.", "2024-02-09", "16:30", "MASCULINO", "ADULTOS", "SIN EMPLEO DE ARMAS", "RURAL", 1),
        ("ANTIOQUIA", "MEDELLÍN", "2024-02-10", "09:45", "MASCULINO", "ADULTOS MAYORES", "SIN EMPLEO DE ARMAS", "URBANA", 1),
    ]
    
    # Multiplicar datos para simular volumen
    data_extended = data * 500  # 10,000 registros
    
    columns = ["DEPARTAMENTO", "MUNICIPIO", "FECHA_HECHO", "HORA_HECHO", 
               "GENERO", "GRUPO_ETARIO", "ARMA_MEDIO", "ZONA", "CANTIDAD"]
    
    df = spark.createDataFrame(data_extended, columns)
    
    print(f"✓ Datos de ejemplo generados")
    print(f"✓ Total de registros: {df.count():,}")
    print(f"✓ Total de columnas: {len(df.columns)}")
    
    return df


# ==============================================================================
# LIMPIEZA Y TRANSFORMACIÓN DE DATOS
# ==============================================================================
def clean_and_transform(df):
    """
    Realiza operaciones de limpieza y transformación sobre el DataFrame.
    
    Args:
        df: DataFrame original
        
    Returns:
        DataFrame: DataFrame limpio y transformado
    """
    print("\n[2] LIMPIEZA Y TRANSFORMACIÓN DE DATOS...")
    print("-" * 50)
    
    # Conteo inicial
    initial_count = df.count()
    
    # Eliminar valores nulos en columnas críticas
    df_clean = df.dropna(subset=["DEPARTAMENTO", "MUNICIPIO", "GENERO"])
    
    # Normalizar valores de texto (mayúsculas)
    df_clean = df_clean.withColumn("DEPARTAMENTO", 
                                    when(col("DEPARTAMENTO").isNotNull(), 
                                         col("DEPARTAMENTO")).otherwise("NO ESPECIFICADO"))
    
    df_clean = df_clean.withColumn("GENERO",
                                    when(col("GENERO").isNotNull(),
                                         col("GENERO")).otherwise("NO ESPECIFICADO"))
    
    # Convertir fecha si es string
    df_clean = df_clean.withColumn("FECHA", to_date(col("FECHA_HECHO"), "yyyy-MM-dd"))
    
    # Extraer componentes de fecha
    df_clean = df_clean.withColumn("ANIO", year(col("FECHA")))
    df_clean = df_clean.withColumn("MES", month(col("FECHA")))
    df_clean = df_clean.withColumn("DIA_SEMANA", dayofweek(col("FECHA")))
    
    # Categorizar hora del día
    df_clean = df_clean.withColumn("HORA_INT", 
                                    col("HORA_HECHO").substr(1, 2).cast(IntegerType()))
    
    df_clean = df_clean.withColumn("FRANJA_HORARIA",
        when((col("HORA_INT") >= 6) & (col("HORA_INT") < 12), "MAÑANA")
        .when((col("HORA_INT") >= 12) & (col("HORA_INT") < 18), "TARDE")
        .when((col("HORA_INT") >= 18) & (col("HORA_INT") < 22), "NOCHE")
        .otherwise("MADRUGADA"))
    
    # Conteo final
    final_count = df_clean.count()
    removed = initial_count - final_count
    
    print(f"✓ Registros iniciales: {initial_count:,}")
    print(f"✓ Registros eliminados (nulos): {removed:,}")
    print(f"✓ Registros finales: {final_count:,}")
    print(f"✓ Nuevas columnas agregadas: FECHA, ANIO, MES, DIA_SEMANA, FRANJA_HORARIA")
    
    return df_clean


# ==============================================================================
# ANÁLISIS EXPLORATORIO DE DATOS (EDA)
# ==============================================================================
def perform_eda(df):
    """
    Realiza análisis exploratorio de datos completo.
    
    Args:
        df: DataFrame limpio
    """
    print("\n[3] ANÁLISIS EXPLORATORIO DE DATOS (EDA)")
    print("=" * 70)
    
    # 3.1 Estadísticas generales
    print("\n3.1 ESTADÍSTICAS GENERALES")
    print("-" * 50)
    print(f"Total de registros: {df.count():,}")
    print(f"Total de columnas: {len(df.columns)}")
    print("\nEsquema del DataFrame:")
    df.printSchema()
    
    # 3.2 Análisis por Departamento
    print("\n3.2 TOP 10 DEPARTAMENTOS CON MÁS HURTOS")
    print("-" * 50)
    dept_stats = df.groupBy("DEPARTAMENTO") \
        .agg(count("*").alias("TOTAL_HURTOS")) \
        .orderBy(desc("TOTAL_HURTOS")) \
        .limit(10)
    dept_stats.show(truncate=False)
    
    # 3.3 Análisis por Género
    print("\n3.3 DISTRIBUCIÓN POR GÉNERO")
    print("-" * 50)
    gender_stats = df.groupBy("GENERO") \
        .agg(count("*").alias("TOTAL")) \
        .withColumn("PORCENTAJE", 
                    spark_round(col("TOTAL") * 100 / df.count(), 2)) \
        .orderBy(desc("TOTAL"))
    gender_stats.show(truncate=False)
    
    # 3.4 Análisis por Grupo Etario
    print("\n3.4 DISTRIBUCIÓN POR GRUPO ETARIO")
    print("-" * 50)
    age_stats = df.groupBy("GRUPO_ETARIO") \
        .agg(count("*").alias("TOTAL")) \
        .withColumn("PORCENTAJE",
                    spark_round(col("TOTAL") * 100 / df.count(), 2)) \
        .orderBy(desc("TOTAL"))
    age_stats.show(truncate=False)
    
    # 3.5 Análisis por Tipo de Arma
    print("\n3.5 DISTRIBUCIÓN POR TIPO DE ARMA/MEDIO")
    print("-" * 50)
    weapon_stats = df.groupBy("ARMA_MEDIO") \
        .agg(count("*").alias("TOTAL")) \
        .withColumn("PORCENTAJE",
                    spark_round(col("TOTAL") * 100 / df.count(), 2)) \
        .orderBy(desc("TOTAL"))
    weapon_stats.show(truncate=False)
    
    # 3.6 Análisis por Zona (Urbana/Rural)
    print("\n3.6 DISTRIBUCIÓN POR ZONA")
    print("-" * 50)
    zone_stats = df.groupBy("ZONA") \
        .agg(count("*").alias("TOTAL")) \
        .withColumn("PORCENTAJE",
                    spark_round(col("TOTAL") * 100 / df.count(), 2)) \
        .orderBy(desc("TOTAL"))
    zone_stats.show(truncate=False)
    
    # 3.7 Análisis por Franja Horaria
    print("\n3.7 DISTRIBUCIÓN POR FRANJA HORARIA")
    print("-" * 50)
    time_stats = df.groupBy("FRANJA_HORARIA") \
        .agg(count("*").alias("TOTAL")) \
        .withColumn("PORCENTAJE",
                    spark_round(col("TOTAL") * 100 / df.count(), 2)) \
        .orderBy(desc("TOTAL"))
    time_stats.show(truncate=False)
    
    # 3.8 Análisis por Día de la Semana
    print("\n3.8 DISTRIBUCIÓN POR DÍA DE LA SEMANA")
    print("-" * 50)
    day_names = {1: "DOMINGO", 2: "LUNES", 3: "MARTES", 4: "MIÉRCOLES", 
                 5: "JUEVES", 6: "VIERNES", 7: "SÁBADO"}
    
    day_stats = df.groupBy("DIA_SEMANA") \
        .agg(count("*").alias("TOTAL")) \
        .orderBy("DIA_SEMANA")
    day_stats.show(truncate=False)
    
    # 3.9 Top 10 Municipios
    print("\n3.9 TOP 10 MUNICIPIOS CON MÁS HURTOS")
    print("-" * 50)
    city_stats = df.groupBy("MUNICIPIO", "DEPARTAMENTO") \
        .agg(count("*").alias("TOTAL_HURTOS")) \
        .orderBy(desc("TOTAL_HURTOS")) \
        .limit(10)
    city_stats.show(truncate=False)
    
    # 3.10 Análisis cruzado: Género vs Arma
    print("\n3.10 ANÁLISIS CRUZADO: GÉNERO vs TIPO DE ARMA")
    print("-" * 50)
    cross_stats = df.groupBy("GENERO", "ARMA_MEDIO") \
        .agg(count("*").alias("TOTAL")) \
        .orderBy(desc("TOTAL")) \
        .limit(10)
    cross_stats.show(truncate=False)
    
    return dept_stats, gender_stats, weapon_stats


# ==============================================================================
# VISUALIZACIÓN DE RESULTADOS
# ==============================================================================
def create_visualizations(df, output_dir="./visualizaciones"):
    """
    Genera visualizaciones de los análisis realizados.
    
    Args:
        df: DataFrame con los datos
        output_dir: Directorio para guardar las visualizaciones
    """
    print("\n[4] GENERANDO VISUALIZACIONES...")
    print("-" * 50)
    
    # Crear directorio si no existe
    os.makedirs(output_dir, exist_ok=True)
    
    # Configurar estilo de matplotlib
    plt.style.use('seaborn-whitegrid')
    plt.rcParams['figure.figsize'] = (12, 6)
    plt.rcParams['font.size'] = 10
    
    # 4.1 Gráfico de barras: Top 10 Departamentos
    dept_pd = df.groupBy("DEPARTAMENTO") \
        .agg(count("*").alias("TOTAL")) \
        .orderBy(desc("TOTAL")) \
        .limit(10) \
        .toPandas()
    
    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.barh(dept_pd['DEPARTAMENTO'], dept_pd['TOTAL'], color='steelblue')
    ax.set_xlabel('Número de Hurtos')
    ax.set_ylabel('Departamento')
    ax.set_title('Top 10 Departamentos con Mayor Cantidad de Hurtos a Personas')
    ax.invert_yaxis()
    
    # Agregar valores en las barras
    for bar, val in zip(bars, dept_pd['TOTAL']):
        ax.text(bar.get_width() + 50, bar.get_y() + bar.get_height()/2, 
                f'{val:,}', va='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/01_top_departamentos.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"✓ Guardado: 01_top_departamentos.png")
    
    # 4.2 Gráfico de pastel: Distribución por Género
    gender_pd = df.groupBy("GENERO") \
        .agg(count("*").alias("TOTAL")) \
        .toPandas()
    
    fig, ax = plt.subplots(figsize=(8, 8))
    colors = ['#3498db', '#e74c3c', '#95a5a6']
    explode = (0.05, 0.05, 0)[:len(gender_pd)]
    ax.pie(gender_pd['TOTAL'], labels=gender_pd['GENERO'], autopct='%1.1f%%',
           colors=colors[:len(gender_pd)], explode=explode[:len(gender_pd)],
           startangle=90, shadow=True)
    ax.set_title('Distribución de Hurtos por Género de la Víctima')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/02_distribucion_genero.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"✓ Guardado: 02_distribucion_genero.png")
    
    # 4.3 Gráfico de barras: Tipo de Arma/Medio
    weapon_pd = df.groupBy("ARMA_MEDIO") \
        .agg(count("*").alias("TOTAL")) \
        .orderBy(desc("TOTAL")) \
        .toPandas()
    
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(range(len(weapon_pd)), weapon_pd['TOTAL'], color='coral')
    ax.set_xticks(range(len(weapon_pd)))
    ax.set_xticklabels(weapon_pd['ARMA_MEDIO'], rotation=45, ha='right')
    ax.set_xlabel('Tipo de Arma/Medio')
    ax.set_ylabel('Número de Hurtos')
    ax.set_title('Distribución de Hurtos por Tipo de Arma/Medio Utilizado')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/03_tipo_arma.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"✓ Guardado: 03_tipo_arma.png")
    
    # 4.4 Gráfico de barras: Franja Horaria
    time_pd = df.groupBy("FRANJA_HORARIA") \
        .agg(count("*").alias("TOTAL")) \
        .toPandas()
    
    # Ordenar franjas
    order = ["MADRUGADA", "MAÑANA", "TARDE", "NOCHE"]
    time_pd['FRANJA_HORARIA'] = pd.Categorical(time_pd['FRANJA_HORARIA'], 
                                                categories=order, ordered=True)
    time_pd = time_pd.sort_values('FRANJA_HORARIA')
    
    fig, ax = plt.subplots(figsize=(10, 6))
    colors = ['#2c3e50', '#f39c12', '#e67e22', '#8e44ad']
    ax.bar(time_pd['FRANJA_HORARIA'], time_pd['TOTAL'], color=colors)
    ax.set_xlabel('Franja Horaria')
    ax.set_ylabel('Número de Hurtos')
    ax.set_title('Distribución de Hurtos por Franja Horaria')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/04_franja_horaria.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"✓ Guardado: 04_franja_horaria.png")
    
    print(f"\n✓ Todas las visualizaciones guardadas en: {output_dir}/")


# ==============================================================================
# DEMOSTRACIÓN DE OPERACIONES RDD
# ==============================================================================
def demonstrate_rdd_operations(spark, df):
    """
    Demuestra operaciones con RDDs como se solicita en la guía.
    
    Args:
        spark: SparkSession
        df: DataFrame para convertir a RDD
    """
    print("\n[5] DEMOSTRACIÓN DE OPERACIONES RDD")
    print("=" * 70)
    
    # Convertir DataFrame a RDD
    rdd = df.rdd
    
    print("\n5.1 TRANSFORMACIÓN: map()")
    print("-" * 50)
    print("Aplicando map para extraer (departamento, 1):")
    dept_rdd = rdd.map(lambda row: (row["DEPARTAMENTO"], 1))
    print(f"Primeros 5 elementos: {dept_rdd.take(5)}")
    
    print("\n5.2 TRANSFORMACIÓN: filter()")
    print("-" * 50)
    print("Filtrando solo registros de BOGOTÁ D.C.:")
    bogota_rdd = rdd.filter(lambda row: "BOGOTÁ" in str(row["MUNICIPIO"]).upper())
    print(f"Total registros Bogotá: {bogota_rdd.count():,}")
    
    print("\n5.3 TRANSFORMACIÓN: reduceByKey()")
    print("-" * 50)
    print("Contando hurtos por departamento con reduceByKey:")
    count_by_dept = dept_rdd.reduceByKey(lambda a, b: a + b)
    top_5 = count_by_dept.sortBy(lambda x: -x[1]).take(5)
    print("Top 5 departamentos:")
    for dept, count in top_5:
        print(f"  - {dept}: {count:,}")
    
    print("\n5.4 ACCIÓN: count()")
    print("-" * 50)
    total = rdd.count()
    print(f"Total de registros en el RDD: {total:,}")
    
    print("\n5.5 ACCIÓN: take()")
    print("-" * 50)
    print("Primeros 3 registros:")
    for i, row in enumerate(rdd.take(3), 1):
        print(f"  {i}. Depto: {row['DEPARTAMENTO']}, Municipio: {row['MUNICIPIO']}, Género: {row['GENERO']}")
    
    print("\n5.6 ACCIÓN: collect() - (muestra limitada)")
    print("-" * 50)
    print("Obteniendo géneros únicos:")
    generos = rdd.map(lambda row: row["GENERO"]).distinct().collect()
    print(f"Géneros encontrados: {generos}")


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================
def main():
    """
    Función principal que orquesta todo el pipeline de procesamiento.
    """
    print("\n")
    print("╔" + "═" * 68 + "╗")
    print("║" + " " * 10 + "TAREA 3 - PROCESAMIENTO DE DATOS CON APACHE SPARK" + " " * 8 + "║")
    print("║" + " " * 20 + "Big Data - Código 202016911" + " " * 21 + "║")
    print("║" + " " * 15 + "Procesamiento Batch - Hurto a Personas" + " " * 14 + "║")
    print("╚" + "═" * 68 + "╝")
    
    # Crear sesión de Spark
    spark = create_spark_session()
    
    try:
        # Cargar datos (usar datos de ejemplo si no hay archivo)
        # Para usar archivo real, descomenta la línea siguiente:
        # df = load_data(spark, "hurto_personas.csv")
        df = generate_sample_data(spark)
        
        # Limpieza y transformación
        df_clean = clean_and_transform(df)
        
        # Cache del DataFrame para mejor rendimiento
        df_clean.cache()
        
        # Análisis exploratorio
        perform_eda(df_clean)
        
        # Generar visualizaciones
        create_visualizations(df_clean)
        
        # Demostrar operaciones RDD
        demonstrate_rdd_operations(spark, df_clean)
        
        # Liberar cache
        df_clean.unpersist()
        
        print("\n" + "=" * 70)
        print("✓ PROCESAMIENTO BATCH COMPLETADO EXITOSAMENTE")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n✗ Error durante el procesamiento: {str(e)}")
        raise
    finally:
        # Cerrar sesión de Spark
        spark.stop()
        print("✓ Sesión de Spark cerrada")


# ==============================================================================
# PUNTO DE ENTRADA
# ==============================================================================
if __name__ == "__main__":
    main()
