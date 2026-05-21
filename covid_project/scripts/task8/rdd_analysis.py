from pyspark.sql import SparkSession
import time

# Create Spark Session
spark = SparkSession.builder \
    .appName("RDD-Based-Analysis") \
    .getOrCreate()

# Create Spark Context
sc = spark.sparkContext

# ---------------------------------------------------
# Read CSV File as RDD
# ---------------------------------------------------

start_rdd = time.time()

rdd = sc.textFile(
    "hdfs:///data/covid/raw/country_wise_latest.csv"
)

# Remove Header
header = rdd.first()

rdd_data = rdd.filter(
    lambda row: row != header
)

# Split CSV Rows
rdd_split = rdd_data.map(
    lambda row: row.split(",")
)

# Total Confirmed Per Country

confirmed_rdd = rdd_split.map(
    lambda x: (x[0], int(x[1]))
)

total_confirmed = confirmed_rdd.reduceByKey(
    lambda a, b: a + b
)

print("\n===== TOTAL CONFIRMED PER COUNTRY =====")

print(total_confirmed.take(10))

# Total Deaths Per Country

deaths_rdd = rdd_split.map(
    lambda x: (x[0], int(x[2]))
)

total_deaths = deaths_rdd.reduceByKey(
    lambda a, b: a + b
)

print("\n===== TOTAL DEATHS PER COUNTRY =====")

print(total_deaths.take(10))

# Death Percentage Per Country

joined_rdd = total_confirmed.join(
    total_deaths
)

death_percentage = joined_rdd.mapValues(
    lambda x: round((x[1] / x[0]) * 100, 2)
)

print("\n===== DEATH PERCENTAGE =====")

print(death_percentage.take(10))

end_rdd = time.time()

rdd_time = end_rdd - start_rdd

print(f"\nRDD Execution Time: {rdd_time:.2f} seconds")

# DataFrame Performance Comparison

start_df = time.time()

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs:///data/covid/raw/country_wise_latest.csv")

df.groupBy("Country/Region").sum("Confirmed").show()

end_df = time.time()

df_time = end_df - start_df

print(f"\nDataFrame Execution Time: {df_time:.2f} seconds")

# Save Results to HDFS

death_percentage_df = death_percentage.toDF(
    ["Country", "Death_Percentage"]
)

death_percentage_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/rdd_death_percentage"
)

print("\n===== TASK 8 ANALYTICS WRITTEN TO HDFS =====")

spark.stop()