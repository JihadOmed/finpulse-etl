from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
import os
from .utils import data_dir

def _spark():
    return (SparkSession.builder
            .appName("YahooFinanceETL")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())

def transform_data(**context):
    spark = _spark()

    raw_path = os.path.join(data_dir(), "raw", "extract.parquet")
    df = spark.read.parquet(raw_path)

    # Calculate features
    w = Window.partitionBy("symbol").orderBy(F.col("date").cast("timestamp"))
    df = df.withColumn("return_1d", (F.col("close") / F.lag("close").over(w) - 1.0))
    df = df.withColumn("moving_avg_7", F.avg("close").over(w.rowsBetween(-6, 0)))
    df = df.withColumn("moving_avg_30", F.avg("close").over(w.rowsBetween(-29, 0)))

    curated_base = os.path.join(data_dir(), "curated")
    os.makedirs(curated_base, exist_ok=True)
    curated_path = os.path.join(curated_base, "prices.parquet")
    df.write.mode("overwrite").parquet(curated_path)

    spark.stop()
    return curated_path
