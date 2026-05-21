from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("COVID-Test") \
    .getOrCreate()

df = spark.read.csv(
    "hdfs:///data/covid/raw/country_wise_latest.csv",
    header=True
)

df.show(5)

spark.stop()
