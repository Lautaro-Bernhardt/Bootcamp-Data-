from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, round as spark_round

spark = SparkSession.builder \
    .appName("CarRentalPipeline") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

# Leer CSVs
df_rental = spark.read.option("header", True).csv("CarRentalData.csv")
df_states = spark.read.option("header", True).csv("georef_states.csv")

# Renombrar columnas: sin espacios, puntos, ni mayúsculas
def clean_column_names(df):
    for col_name in df.columns:
        df = df.withColumnRenamed(
            col_name,
            col_name.strip().lower().replace(" ", "_").replace(".", "_")
        )
    return df

df_rental = clean_column_names(df_rental)
df_states = clean_column_names(df_states)

# Normalizar fueltype, redondear rating
df_rental = df_rental.withColumn("fueltype", lower(col("fueltype")))
df_rental = df_rental.withColumn("rating", spark_round(col("rating")).cast("int"))

# Filtrar rating no nulo y no Texas
df_rental = df_rental.filter(col("rating").isNotNull())
df_rental = df_rental.filter(col("statename") != "Texas")

# Join entre state (código) y state_code
df_joined = df_rental.join(
    df_states,
    df_rental["state"] == df_states["state_code"],
    how="left"
).drop("state", "state_code") \
 .withColumnRenamed("name", "state_name")

# Seleccionar columnas finales para guardar
final_df = df_joined.select(
    "fueltype", "rating", "rentertripstaken", "reviewcount",
    "city", "state_name", "owner_id", "rate_daily",
    "make", "model", "year"
)

# Insertar en Hive
final_df.write.mode("overwrite").saveAsTable("car_rental_db.car_rental_analytics")

print("✅ Datos procesados y cargados correctamente en Hive")

spark.stop()

