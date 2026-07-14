import json
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.functions import round as sparkRound


def runDataFramePipeline(inputPath, outputPath):
    spark = SparkSession.builder.appName("DataFramePipelineNormalizedStats").getOrCreate()

    start = time.perf_counter()
    df = spark.read.text(inputPath).selectExpr("CAST(value AS FLOAT) as value")
    grouped = df.withColumn("bucket", sparkRound(col("value"), 1)).groupBy("bucket").agg(count("*").alias("total"))
    stats = {str(row["bucket"]): row["total"] for row in grouped.collect()}
    elapsed = time.perf_counter() - start

    result = {"pipeline": "dataframe", "bucketCounts": stats, "elapsedSeconds": elapsed}
    spark.sparkContext.parallelize([json.dumps(result)]).saveAsTextFile(outputPath)
    spark.stop()


if __name__ == "__main__":
    runDataFramePipeline(sys.argv[1], sys.argv[2])
