from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    round,
    desc,
    avg
)

# Create Spark Session
spark = SparkSession.builder \
    .appName("COVID-Infection-Analysis") \
    .getOrCreate()

# Read Dataset from HDFS
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs:///data/covid/raw/worldometer_data.csv")

# 1. Confirmed Cases Per 1000 Population

confirmed_df = df.withColumn(
    "Confirmed_Per_1000",
    round(
        (col("TotalCases") / col("Population")) * 1000,
        2
    )
)

print("\n===== CONFIRMED CASES PER 1000 =====")

confirmed_df.select(
    "Country/Region",
    "TotalCases",
    "Population",
    "Confirmed_Per_1000"
).show(10)

# 2. Active Cases Per 1000 Population

active_df = confirmed_df.withColumn(
    "Active_Per_1000",
    round(
        (col("ActiveCases") / col("Population")) * 1000,
        2
    )
)

print("\n===== ACTIVE CASES PER 1000 =====")

active_df.select(
    "Country/Region",
    "ActiveCases",
    "Population",
    "Active_Per_1000"
).show(10)

# 3. Top 10 Countries by Infection Rate

top10_df = active_df.orderBy(
    desc("Confirmed_Per_1000")
)

print("\n===== TOP 10 COUNTRIES BY INFECTION RATE =====")

top10_df.select(
    "Country/Region",
    "Confirmed_Per_1000"
).show(10)

# 4. WHO Region Infection Ranking

region_df = active_df.groupBy("WHO Region").agg(
    avg("Confirmed_Per_1000").alias("Avg_Infection_Rate")
)

region_df = region_df.orderBy(
    desc("Avg_Infection_Rate")
)

print("\n===== WHO REGION INFECTION RANKING =====")

region_df.show()

# Write Results to HDFS

confirmed_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/confirmed_per_1000"
)

active_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/active_per_1000"
)

top10_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/top10_infection_rate"
)

region_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/who_region_ranking"
)

print("\n===== TASK 4 ANALYTICS WRITTEN TO HDFS =====")

spark.stop()