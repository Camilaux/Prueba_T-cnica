import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

# Inicializar Spark para procesamiento
spark = SparkSession.builder \
    .appName("TransactionMonitoringETL") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# 1. EXTRACCIÓN
print("Iniciando extracción de datos...")
# Leer dataset (simulando stream con procesamiento en micro-batches)
data_df = spark.read.csv("data.csv", header=True, inferSchema=True)

print(data_df)

# 2. TRANSFORMACIÓN
print("Iniciando transformación de datos...")

# Convertir a DataFrame para manipulación más fácil
transactions_df = data_df.withColumn(
    "transaction_datetime", 
    to_timestamp(
        concat(
            col("finaltrxyear"), lit("-"), 
            col("finaltrxmonth"), lit("-"), 
            col("finaltrxday"), lit(" "), 
            col("finaltrxhour"), lit(":00:00")
        )
    )
)

# Convertir timestamp a segundos para poder usar rangeBetween
transactions_df = transactions_df.withColumn(
    "transaction_time_seconds", 
    unix_timestamp("transaction_datetime")
)

# Crear indicador de éxito de transacción
transactions_df = transactions_df.withColumn(
    "is_successful", 
    when(col("responsecode") == 0, 1).otherwise(0)
)

# Categorizar transacciones
transactions_df = transactions_df.withColumn(
    "transaction_category", 
    when(col("transactiontype").contains("MONETARIA"), "FINANCIAL")
    .otherwise("NON_FINANCIAL")
)

# Usar la columna de segundos para la ventana
window_specs = Window.partitionBy("channel").orderBy("transaction_time_seconds").rangeBetween(-60, 0)  # 60 segundos anteriores

# Crear métricas por canal y ventana de tiempo
metrics_df = transactions_df.withColumn(
    "transactions_per_minute", count("*").over(window_specs)
).withColumn(
    "success_rate", 
    sum("is_successful").over(window_specs) / count("*").over(window_specs) * 100
).withColumn(
    "error_rate", 
    (1 - sum("is_successful").over(window_specs) / count("*").over(window_specs)) * 100
)

# 3. CARGA
print("Cargando datos procesados...")

# Función para cargar en almacenamiento "caliente" (Redis - simulado)
def load_to_hot_storage(batch_df):
    # Aquí conectaríamos con Redis para guardar métricas recientes
    batch_df.persist()  # En un entorno real, esto sería un write a Redis
    print(f"Hot data loaded: {batch_df.count()} records")

# Función para cargar en almacenamiento "tibio" (TimescaleDB - simulado)
def load_to_warm_storage(batch_df):
    # Aquí conectaríamos con TimescaleDB
    batch_df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/metrics") \
        .option("dbtable", "transaction_metrics") \
        .option("user", "postgres") \
        .option("password", "password") \
        .mode("append") \
        .save()
    print(f"Warm data loaded: {batch_df.count()} records")

# En un entorno real, estas serían operaciones de streaming
load_to_hot_storage(metrics_df)

# 4. ANÁLISIS
print("Ejecutando análisis en tiempo real...")

# Modelo de Detección de Anomalías (simplificado)
def detect_anomalies(df):
    # Calculamos valores estadísticos para detección de anomalías
    stats = df.groupBy("channel").agg(
        avg("transactions_per_minute").alias("avg_tpm"),
        stddev("transactions_per_minute").alias("stddev_tpm"),
        avg("error_rate").alias("avg_error"),
        stddev("error_rate").alias("stddev_error")
    )
    
    # Unimos con métricas originales
    anomaly_df = df.join(stats, on="channel")
    
    # Marcamos anomalías (Z-score > 3)
    anomaly_df = anomaly_df.withColumn(
        "volume_anomaly",
        when(
            abs((col("transactions_per_minute") - col("avg_tpm")) / col("stddev_tpm")) > 3,
            1
        ).otherwise(0)
    ).withColumn(
        "error_anomaly",
        when(
            abs((col("error_rate") - col("avg_error")) / col("stddev_error")) > 3,
            1
        ).otherwise(0)
    )
    
    return anomaly_df

# Aplicar detección de anomalías
anomaly_df = detect_anomalies(metrics_df)

# Mostrar canales con anomalías
anomalies = anomaly_df.filter(
    (col("volume_anomaly") == 1) | (col("error_anomaly") == 1)
)

# 5. VISUALIZACIÓN (simulada para dashboards)
print("Generando visualizaciones...")

# En un entorno real, estos datos se enviarían a Grafana

# Métricas globales
global_metrics = metrics_df.groupBy(
    window(col("transaction_datetime"), "1 minute")
).agg(
    avg("transactions_per_minute").alias("avg_tpm"),
    avg("success_rate").alias("avg_success_rate"),
    avg("error_rate").alias("avg_error_rate")
)

# Métricas por canal
channel_metrics = metrics_df.groupBy(
    "channel", window(col("transaction_datetime"), "1 minute")
).agg(
    avg("transactions_per_minute").alias("avg_tpm"),
    avg("success_rate").alias("avg_success_rate"),
    avg("error_rate").alias("avg_error_rate")
)

print("ETL completado exitosamente")