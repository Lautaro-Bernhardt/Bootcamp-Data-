from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, round as spark_round, sum as spark_sum, avg

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("CarRentalQueries") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.table("car_rental_db.car_rental_analytics")

print("\nConsulta a) Total de alquileres ecológicos con rating >= 4:")
df.filter(
    (lower(col("fueltype")).isin("hybrid", "electric")) &
    (col("rating") >= 4)
).selectExpr("SUM(rentertripstaken) AS total_alquileres_ecologicos").show()

print("\nConsulta b) Los 5 estados con menor cantidad de alquileres:")
df.groupBy("state_name") \
    .agg(spark_sum("rentertripstaken").alias("total_alquileres")) \
    .orderBy("total_alquileres") \
    .show(5)

print("\nConsulta c) Los 10 modelos más rentados:")
df.groupBy("make", "model") \
    .agg(spark_sum("rentertripstaken").alias("total_rentas")) \
    .orderBy(col("total_rentas").desc()) \
    .show(10)

print("\nConsulta d) Alquileres por año de vehículos fabricados entre 2010 y 2015:")
df.filter((col("year") >= 2010) & (col("year") <= 2015)) \
  .groupBy("year") \
  .agg(spark_sum("rentertripstaken").alias("alquileres_por_año")) \
  .orderBy("year") \
  .show()

print("\nConsulta e) Las 5 ciudades con más alquileres de vehículos ecológicos:")
df.filter(lower(col("fueltype")).isin("hybrid", "electric")) \
  .groupBy("city") \
  .agg(spark_sum("rentertripstaken").alias("alquileres_ecologicos")) \
  .orderBy(col("alquileres_ecologicos").desc()) \
  .show(5)

print("\nConsulta f) Promedio de reviews por tipo de combustible:")
df.groupBy("fueltype") \
  .agg(avg("reviewcount").alias("promedio_reviews")) \
  .orderBy("fueltype") \
  .show()

spark.stop()

