from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

# Crear sesión de Spark
spark = SparkSession.builder.appName("Analisis de Vuelos 2021").getOrCreate()

# Cargar datasets
df_vuelos = spark.read.option("header", "true").option("delimiter", ";").csv("data/informe_2021.csv")
df_aeropuertos = spark.read.option("header", "true").option("delimiter", ";").csv("data/aeropuertos_detalle.csv")

# Renombrar columnas con espacios y caracteres especiales
df_vuelos = df_vuelos.withColumnRenamed("Clase de Vuelo (todos los vuelos)", "clase_vuelo") \
                     .withColumnRenamed("Clasificación Vuelo", "clasificacion_vuelo") \
                     .withColumnRenamed("Tipo de Movimiento", "tipo_movimiento") \
                     .withColumnRenamed("Aeropuerto", "aeropuerto_iata") \
                     .withColumnRenamed("Origen / Destino", "aeropuerto_oaci")

# Join entre vuelos y detalle de aeropuertos
df_join = df_vuelos.join(
    df_aeropuertos,
    df_vuelos["aeropuerto_iata"] == df_aeropuertos["iata"],
    how="inner"
)

# 1. Cantidad de vuelos internacionales
vuelos_internacionales = df_vuelos.filter(col("clasificacion_vuelo") == "Internacional").count()
print(f"✈️ Vuelos internacionales: {vuelos_internacionales}")

# 2. Top 5 aeropuertos con más vuelos
print("\n🏆 Top 5 aeropuertos con más vuelos:")
df_join.groupBy("aeropuerto_iata", "denominacion", "provincia") \
       .count() \
       .orderBy("count", ascending=False) \
       .show(5)

# 3. Promedio de vuelos por aeropuerto
print("\n📊 Promedio de vuelos por aeropuerto:")
df_join.groupBy("aeropuerto_iata").count().agg(avg("count").alias("promedio")).show()

# 4. Top 5 provincias con más vuelos
print("\n🗺️ Top 5 provincias con más vuelos:")
df_join.groupBy("provincia") \
       .agg(count("*").alias("total_vuelos")) \
       .orderBy("total_vuelos", ascending=False) \
       .show(5)

# Finalizar la sesión
spark.stop()
