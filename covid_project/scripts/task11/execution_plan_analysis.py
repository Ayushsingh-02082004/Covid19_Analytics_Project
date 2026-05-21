from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# ===================================================
# CREATE SPARK SESSION
# ===================================================

spark = SparkSession.builder \
    .appName("Execution-Plan-Analysis") \
    .getOrCreate()

# ===================================================
# READ DATASETS FROM HDFS
# ===================================================

covid_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs:///data/covid/raw/full_grouped.csv")

worldometer_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs:///data/covid/raw/worldometer_data.csv")

# ===================================================
# QUERY 1 — GROUPBY ANALYSIS
# ===================================================

group_query = covid_df.groupBy(
    "Country/Region"
).sum("Confirmed")

print("\n===== QUERY 1 : GROUPBY EXECUTION PLAN =====")

group_query.explain("extended")

# ===================================================
# QUERY 2 — BROADCAST HASH JOIN
# ===================================================

filtered_worldometer = worldometer_df.select(
    "Country/Region",
    "Population"
)

broadcast_query = covid_df.join(
    broadcast(filtered_worldometer),
    on="Country/Region",
    how="inner"
)

print("\n===== QUERY 2 : BROADCAST JOIN EXECUTION PLAN =====")

broadcast_query.explain("extended")

# ===================================================
# QUERY 3 — SORT MERGE JOIN
# ===================================================

sort_merge_query = covid_df.join(
    worldometer_df,
    on="Country/Region",
    how="inner"
)

print("\n===== QUERY 3 : SORT MERGE JOIN EXECUTION PLAN =====")

sort_merge_query.explain("extended")

# ===================================================
# SHOW SAMPLE OUTPUT
# ===================================================

print("\n===== SAMPLE GROUP QUERY OUTPUT =====")

group_query.show(5)

print("\n===== SAMPLE BROADCAST JOIN OUTPUT =====")

broadcast_query.show(5)

print("\n===== SAMPLE SORT MERGE JOIN OUTPUT =====")

sort_merge_query.show(5)

# ===================================================
# STOP SPARK SESSION
# ===================================================

spark.stop()