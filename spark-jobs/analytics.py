import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, avg, stddev, lag, round, when, row_number,
    current_timestamp, expr,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .appName("CryptoAnalytics") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

JDBC_URL = "jdbc:postgresql://postgres:5432/crypto_metrics"
DB_PROPS = {
    "user": "postgres",
    "password": "admin",
    "driver": "org.postgresql.Driver",
}

PARQUET_PATH = "s3a://crypto-data/parquet/"

try:
    df = spark.read.parquet(PARQUET_PATH).drop(
        "market_cap", "total_volume", "high_24h", "low_24h", "last_updated"
    )

    df = df.withColumn("timestamp", to_timestamp("timestamp")) \
           .withColumn("price", col("price").cast(DoubleType()))

    latest_data = df.filter(
        col("timestamp") >= current_timestamp() - expr("INTERVAL 7 MINUTES")
    )
    row_count = latest_data.count()

    if row_count == 0:
        print("No data found in the last 7 minutes. Exiting.")
        spark.stop()
        sys.exit(0)

    coin_window = Window.partitionBy("id").orderBy("timestamp")

    # Price changes
    latest_data = latest_data \
        .withColumn("price_1min_ago", lag("price", 2).over(coin_window)) \
        .withColumn("price_5min_ago", lag("price", 10).over(coin_window)) \
        .withColumn(
            "change_1min",
            when(col("price_1min_ago").isNull(), None)
            .otherwise(round((col("price") - col("price_1min_ago")) / col("price_1min_ago") * 100, 2)),
        ) \
        .withColumn(
            "change_5min",
            when(col("price_5min_ago").isNull(), None)
            .otherwise(round((col("price") - col("price_5min_ago")) / col("price_5min_ago") * 100, 2)),
        ) \
        .drop("price_1min_ago", "price_5min_ago")

    # SMA & EMA
    latest_data = latest_data \
        .withColumn("SMA", avg("price").over(coin_window.rowsBetween(-4, 0))) \
        .withColumn("EMA", avg("price").over(coin_window.rowsBetween(-2, 0)))

    # Volatility
    latest_data = latest_data \
        .withColumn("volatility", stddev("price").over(coin_window.rowsBetween(-4, 0)))

    # Latest record per coin
    latest_per_coin = latest_data \
        .withColumn("row_num", row_number().over(
            Window.partitionBy("id").orderBy(col("timestamp").desc())
        )) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    # Top 5 gainers & losers
    gain_df = latest_per_coin \
        .filter(col("change_5min").isNotNull()) \
        .withColumn("rank", row_number().over(Window.orderBy(col("change_5min").desc()))) \
        .filter(col("rank") <= 5) \
        .select("rank", "id", "symbol")

    loss_df = latest_per_coin \
        .filter(col("change_5min").isNotNull()) \
        .withColumn("rank", row_number().over(Window.orderBy(col("change_5min").asc()))) \
        .filter(col("rank") <= 5) \
        .select("rank", "id", "symbol")

    # Console output
    print("\n=== All Metrics ===")
    latest_data.select(
        "timestamp", "id", "symbol", "price",
        "change_1min", "change_5min", "SMA", "EMA", "volatility",
    ).orderBy("timestamp").show(truncate=False)

    print("=== Top 5 Gainers ===")
    gain_df.show(truncate=False)

    print("=== Top 5 Losers ===")
    loss_df.show(truncate=False)

    # Write to PostgreSQL
    latest_data.select(
        "timestamp", "id", "symbol", "price",
        "change_1min", "change_5min", "SMA", "EMA", "volatility",
    ).write.jdbc(url=JDBC_URL, table="crypto_table", mode="append", properties=DB_PROPS)

    gain_df.write.jdbc(url=JDBC_URL, table="top_5_gainers", mode="overwrite", properties=DB_PROPS)
    loss_df.write.jdbc(url=JDBC_URL, table="top_5_losers", mode="overwrite", properties=DB_PROPS)

    print("Data written to PostgreSQL.")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()