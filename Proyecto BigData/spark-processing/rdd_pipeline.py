import json
import sys
import time

from pyspark import SparkConf, SparkContext


def runRDDPipeline(inputPath, outputPath):
    conf = SparkConf().setAppName("RDDPipelineNormalizedStats")
    sc = SparkContext.getOrCreate(conf=conf)

    start = time.perf_counter()
    rdd = sc.textFile(inputPath).map(lambda line: float(line))
    grouped = rdd.map(lambda x: (round(x, 1), 1)).reduceByKey(lambda a, b: a + b)
    stats = grouped.collect()
    elapsed = time.perf_counter() - start

    result = {"pipeline": "rdd", "bucketCounts": dict(stats), "elapsedSeconds": elapsed}
    sc.parallelize([json.dumps(result)]).saveAsTextFile(outputPath)
    sc.stop()


if __name__ == "__main__":
    runRDDPipeline(sys.argv[1], sys.argv[2])
