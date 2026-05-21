from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    round,
    lag,
    row_number,
    avg,
    max,
    desc
)

from pyspark.sql.window import Window

# Create Spark Session
spark = SparkSession.builder \
    .appName("COVID-Recovery-Analysis") \
    .getOrCreate()

# Read Dataset from HDFS
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs:///data/covid/raw/full_grouped.csv")

# 1. Recovery Percentage Per Country

recovery_df = df.withColumn(
    "Recovery_Percentage",
    round(
        (col("Recovered") / col("Confirmed")) * 100,
        2
    )
)

print("\n===== RECOVERY PERCENTAGE =====")

recovery_df.select(
    "Date",
    "Country/Region",
    "Confirmed",
    "Recovered",
    "Recovery_Percentage"
).show(10)

# 2. 7-Day Rolling Recovery Average
# ---------------------------------------------------

window_spec = Window.partitionBy(
    "Country/Region"
).orderBy("Date").rowsBetween(-6, 0)

rolling_df = recovery_df.withColumn(
    "Rolling_7Day_Recovery_Avg",
    round(
        avg("Recovery_Percentage").over(window_spec),
        2
    )
)

print("\n===== 7-DAY ROLLING RECOVERY AVERAGE =====")

rolling_df.select(
    "Date",
    "Country/Region",
    "Recovery_Percentage",
    "Rolling_7Day_Recovery_Avg"
).show(10)

# 3. Country with Fastest Recovery Growth
# ---------------------------------------------------

growth_window = Window.partitionBy(
    "Country/Region"
).orderBy("Date")

growth_df = rolling_df.withColumn(
    "Previous_Recovered",
    lag("Recovered", 1).over(growth_window)
)

growth_df = growth_df.withColumn(
    "Recovery_Growth",
    col("Recovered") - col("Previous_Recovered")
)

fastest_growth_df = growth_df.orderBy(
    desc("Recovery_Growth")
)

print("\n===== FASTEST RECOVERY GROWTH =====")

fastest_growth_df.select(
    "Date",
    "Country/Region",
    "Recovery_Growth"
).show(10)

# 4. Peak Recovery Day Per Country
# ---------------------------------------------------

peak_window = Window.partitionBy(
    "Country/Region"
).orderBy(desc("Recovered"))

peak_df = growth_df.withColumn(
    "rank",
    row_number().over(peak_window)
)

peak_recovery_df = peak_df.filter(
    col("rank") == 1
)

print("\n===== PEAK RECOVERY DAY =====")

peak_recovery_df.select(
    "Country/Region",
    "Date",
    "Recovered"
).show(10)

# Write Results to HDFS
# ---------------------------------------------------

recovery_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/recovery_percentage"
)

rolling_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/rolling_recovery_average"
)

fastest_growth_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/fastest_recovery_growth"
)

peak_recovery_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/peak_recovery_day"
)

print("\n===== TASK 5 ANALYTICS WRITTEN TO HDFS =====")

spark.stop()