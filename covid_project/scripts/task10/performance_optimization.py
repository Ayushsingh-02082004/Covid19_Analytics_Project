from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.storagelevel import StorageLevel

# Create Spark Session
spark = SparkSession.builder \
    .appName("Performance-Optimization") \
    .getOrCreate()

# Shuffle Partition Tuning
# ---------------------------------------------------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# Read Datasets

covid_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs:///data/covid/raw/full_grouped.csv")

worldometer_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs:///data/covid/raw/worldometer_data.csv")

# Caching Strategy

covid_df.persist(StorageLevel.MEMORY_AND_DISK)

print("\n===== DATAFRAME CACHED =====")

# Partition Strategy

repartitioned_df = covid_df.repartition(
    "Date",
    "Country/Region"
)

# Write Partitioned Parquet

repartitioned_df.write \
    .mode("overwrite") \
    .partitionBy("Date") \
    .parquet(
        "hdfs:///data/covid/analytics/optimized_partitioned_data"
    )

print("\n===== PARTITIONED PARQUET WRITTEN =====")

# Detect Skew

skew_df = covid_df.groupBy(
    "Country/Region"
).count().orderBy("count", ascending=False)

print("\n===== SKEW ANALYSIS =====")

skew_df.show(10)

# Broadcast Join Optimization

filtered_worldometer = worldometer_df.select(
    "Country/Region",
    "Population"
).filter(
    worldometer_df["Country/Region"].isNotNull()
)

optimized_join = repartitioned_df.join(
    broadcast(filtered_worldometer),
    on="Country/Region",
    how="inner"
)

print("\n===== BROADCAST JOIN EXECUTION PLAN =====")

optimized_join.explain("formatted")

# Avoid Unnecessary Wide Transformations

filtered_df = covid_df.filter(
    covid_df["Confirmed"] > 1000
).filter(
    covid_df["Deaths"] > 100
)

print("\n===== FILTERED DATA =====")

filtered_df.show(10)

# Save Optimized Output

optimized_join.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/broadcast_join_output"
)

print("\n===== TASK 10 COMPLETED =====")

spark.stop()