
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)
from pyspark.sql.functions import col

# Create Spark Session
spark = SparkSession.builder \
    .appName("COVID-CSV-to-Parquet") \
    .getOrCreate()

# Define Schema
schema = StructType([
    StructField("Country/Region", StringType(), True),
    StructField("Confirmed", IntegerType(), True),
    StructField("Deaths", IntegerType(), True),
    StructField("Recovered", IntegerType(), True),
    StructField("Active", IntegerType(), True),
    StructField("New cases", IntegerType(), True),
    StructField("New deaths", IntegerType(), True),
    StructField("New recovered", IntegerType(), True),
    StructField("Deaths / 100 Cases", DoubleType(), True),
    StructField("Recovered / 100 Cases", DoubleType(), True),
    StructField("Deaths / 100 Recovered", DoubleType(), True),
    StructField("Confirmed last week", IntegerType(), True),
    StructField("1 week change", IntegerType(), True),
    StructField("1 week % increase", DoubleType(), True),
    StructField("WHO Region", StringType(), True)
])

# Read CSV from HDFS
df = spark.read \
    .option("header", True) \
    .schema(schema) \
    .csv("hdfs:///data/covid/raw/country_wise_latest.csv")

# Show Schema
print("\n===== SCHEMA =====")
df.printSchema()

# Show Data
print("\n===== SAMPLE DATA =====")
df.show(5)

# Handle Null Values
df_clean = df.fillna({
    "Confirmed": 0,
    "Deaths": 0,
    "Recovered": 0,
    "Active": 0
})

# Write Parquet to HDFS
df_clean.write \
    .mode("overwrite") \
    .parquet("hdfs:///data/covid/staging/country_wise_parquet")

print("\n===== PARQUET FILE WRITTEN SUCCESSFULLY =====")

# Read Parquet
parquet_df = spark.read.parquet(
    "hdfs:///data/covid/staging/country_wise_parquet"
)

print("\n===== PARQUET DATA =====")
parquet_df.show(5)

# Compare Execution Plans
print("\n===== CSV EXECUTION PLAN =====")
df.explain()

print("\n===== PARQUET EXECUTION PLAN =====")
parquet_df.explain()

spark.stop()
