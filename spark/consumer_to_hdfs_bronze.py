from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_date
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# --- Initialiser SparkSession avec support Kafka ---
spark = SparkSession.builder \
    .appName("KafkaToHDFS_Bronze") \
    .getOrCreate()

# --- Définir le schéma des données ---
schema = StructType([
    StructField("uid", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("lat", FloatType(), True),
    StructField("lon", FloatType(), True),
    StructField("aqi", StringType(), True),
    StructField("dominant_pollutant", StringType(), True),
    StructField("pm25", FloatType(), True),
    StructField("pm10", FloatType(), True),
    StructField("no2", FloatType(), True),
    StructField("o3", FloatType(), True),
    StructField("so2", FloatType(), True),
    StructField("co", FloatType(), True),
    StructField("time", StringType(), True)
])

# --- Lire depuis Kafka ---
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stations_air") \
    .option("startingOffsets", "earliest") \
    .load()

# --- Convertir la valeur JSON en colonnes ---
df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# --- Ajouter une colonne date pour partitionnement ---
df = df.withColumn("date", current_date())

# --- Écrire dans HDFS en JSON partitionné par date ---
df.write.mode("append") \
    .partitionBy("date") \
    .json("hdfs://hdfs_namenode:9000/datalake/bronze/waqi")

print("✅ Données stockées dans HDFS en Bronze avec partition par date")

