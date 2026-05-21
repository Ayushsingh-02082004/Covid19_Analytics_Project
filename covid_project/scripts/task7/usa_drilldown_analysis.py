from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    desc,
    count,avg
)

# Create Spark Session
spark = SparkSession.builder \
    .appName("USA-Drilldown-Analysis") \
    .getOrCreate()

# Read Dataset from HDFS
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs:///data/covid/raw/usa_county_wise.csv")


# 1. Aggregate County Data to State Level

state_df = df.groupBy("Province_State").agg(
    sum("Confirmed").alias("Total_Confirmed"),
    sum("Deaths").alias("Total_Deaths")
)

print("\n===== STATE LEVEL AGGREGATION =====")

state_df.show(10)

# 2. Top 10 Affected States

top10_states_df = state_df.orderBy(
    desc("Total_Confirmed")
)

print("\n===== TOP 10 AFFECTED STATES =====")

top10_states_df.select(
    "Province_State",
    "Total_Confirmed",
    "Total_Deaths"
).show(10)

# 3. Detect Data Skew Across States

skew_df = df.groupBy("Province_State").agg(
    count("*").alias("County_Record_Count")
)

skew_df = skew_df.orderBy(
    desc("County_Record_Count")
)

print("\n===== DATA SKEW ANALYSIS =====")

skew_df.show(20)

# 4. Skew Ratio Calculation

max_count = skew_df.agg(
    {"County_Record_Count": "max"}
).collect()[0][0]

avg_count = skew_df.agg(
    avg("County_Record_Count")
).collect()[0][0]

skew_ratio = round(max_count / avg_count, 2)

print("\n===== SKEW RATIO =====")

print(f"Skew Ratio: {skew_ratio}")

# Write Results to HDFS

state_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/state_level_aggregation"
)

top10_states_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/top10_affected_states"
)

skew_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/state_data_skew"
)

print("\n===== TASK 7 ANALYTICS WRITTEN TO HDFS =====")

spark.stop()