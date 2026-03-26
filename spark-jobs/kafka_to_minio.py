from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import (
   StructType, StructField, StringType, DoubleType, LongType, ArrayType,
)


# Schema for individual coin
coin_schema = StructType([
   StructField("id", StringType(), True),
   StructField("symbol", StringType(), True),
   StructField("current_price", DoubleType(), True),
   StructField("market_cap", LongType(), True),
   StructField("total_volume", DoubleType(), True),
   StructField("high_24h", DoubleType(), True),
   StructField("low_24h", DoubleType(), True),
   StructField("last_updated", StringType(), True),
])


# Full Kafka message schema
schema = StructType([
   StructField("timestamp", StringType(), True),
   StructField("data", ArrayType(coin_schema), True),
])


spark = SparkSession.builder \
   .appName("KafkaToMinIO") \
   .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
   .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
   .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
   .config("spark.hadoop.fs.s3a.path.style.access", "true") \
   .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
   .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")


# Read from Kafka
raw_df = spark.readStream \
   .format("kafka") \
   .option("kafka.bootstrap.servers", "kafka:9092") \
   .option("subscribe", "crypto-prices") \
   .option("startingOffsets", "earliest") \
   .option("failOnDataLoss", "false") \
   .load()


# Parse and flatten
json_df = raw_df.selectExpr("CAST(value AS STRING) as json")
parsed_df = json_df.select(from_json(col("json"), schema).alias("parsed"))


flattened_df = parsed_df.select(
   col("parsed.timestamp").alias("timestamp"),
   explode(col("parsed.data")).alias("coin"),
).select(
   "timestamp",
   col("coin.id").alias("id"),
   col("coin.symbol").alias("symbol"),
   col("coin.current_price").alias("price"),
   col("coin.market_cap"),
   col("coin.total_volume"),
   col("coin.high_24h"),
   col("coin.low_24h"),
   col("coin.last_updated"),
)


# Write Parquet to MinIO
query = flattened_df.writeStream \
   .format("parquet") \
   .outputMode("append") \
   .trigger(processingTime="30 seconds") \
   .option("path", "s3a://crypto-data/parquet/") \
   .option("checkpointLocation", "s3a://crypto-data/checkpoints/") \
   .start()


query.awaitTermination()