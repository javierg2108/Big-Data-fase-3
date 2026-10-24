from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, desc

# 1. Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisBatchCalidadAire") \
    .getOrCreate()

# 2. Cargar el conjunto de datos desde el archivo CSV
df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("air_quality_data.csv")

print("--- Esquema Original del Dataset ---")
df.printSchema()
print("Columnas disponibles:", df.columns)

# 3. Limpieza y Transformación de Datos (Renombramos Solar.R para evitar el punto)
df = df.withColumnRenamed("Solar.R", "SolarR")
df_cleaned = df.dropna().filter(col("Temp") > 0)

# 4. Análisis Exploratorio de Datos (EDA) y Agregaciones
print("--- Estadísticas descriptivas por Mes ---")
summary_df = df_cleaned.groupBy("Month") \
    .agg(
        avg("Ozone").alias("promedio_ozono"),
        max("Ozone").alias("maximo_ozono"),
        avg("Temp").alias("promedio_temperatura")
    ) \
    .orderBy(desc("promedio_ozono"))

# 5. Visualización de los resultados del procesamiento en batch
summary_df.show(10)

# Guardar los resultados procesados
summary_df.write.mode("overwrite").option("header", "true").csv("resultados_batch_salida")

spark.stop()