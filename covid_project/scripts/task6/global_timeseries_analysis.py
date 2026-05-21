from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    max,
    stddev,
    round,
    desc,
    month,
    year,
    lag
)

from pyspark.sql.window import Window

# Create Spark Session
spark = SparkSession.builder \
    .appName("Global-TimeSeries-Analysis") \
    .getOrCreate()

# Read Dataset from HDFS
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs:///data/covid/raw/day_wise.csv")

# ---------------------------------------------------
# 1. Global Daily Average New Cases
# ---------------------------------------------------

avg_cases_df = df.select(
    avg("New cases").alias("Global_Avg_New_Cases")
)

print("\n===== GLOBAL DAILY AVERAGE NEW CASES =====")  

avg_cases_df.show()

# ---------------------------------------------------
# 2. Detect Spike Days Using Z-Score
# ---------------------------------------------------

stats = df.select(
    avg("New cases").alias("mean"),
    stddev("New cases").alias("std")
).collect()[0]

mean_value = stats["mean"]
std_value = stats["std"]

zscore_df = df.withColumn(
    "Z_Score",
    round(
        (col("New cases") - mean_value) / std_value,
        2
    )
)

spike_df = zscore_df.filter(
    col("Z_Score") > 2
)

print("\n===== SPIKE DAYS USING Z-SCORE =====")

spike_df.select(
    "Date",
    "New cases",
    "Z_Score"
).show(10)

# ---------------------------------------------------
# 3. Peak Death Date Globally
# ---------------------------------------------------

peak_death_df = df.orderBy(
    desc("Deaths")
)

print("\n===== PEAK GLOBAL DEATH DATE =====")

peak_death_df.select(
    "Date",
    "Deaths"
).show(1)

# ---------------------------------------------------
# 4. Month-over-Month Death Growth Rate
# ---------------------------------------------------

monthly_df = df.withColumn(
    "Month",
    month("Date")
).withColumn(
    "Year",
    year("Date")
)

monthly_deaths_df = monthly_df.groupBy(
    "Year",
    "Month"
).agg(
    max("Deaths").alias("Monthly_Deaths")
)

window_spec = Window.orderBy(
    "Year",
    "Month"
)

monthly_growth_df = monthly_deaths_df.withColumn(
    "Previous_Month_Deaths",
    lag("Monthly_Deaths", 1).over(window_spec)
)

monthly_growth_df = monthly_growth_df.withColumn(
    "Death_Growth_Rate",
    round(
        (
            (
                col("Monthly_Deaths") -
                col("Previous_Month_Deaths")
            )
            / col("Previous_Month_Deaths")
        ) * 100,
        2
    )
)

print("\n===== MONTH-OVER-MONTH DEATH GROWTH =====")

monthly_growth_df.show()

# ---------------------------------------------------
# Write Results to HDFS
# ---------------------------------------------------

avg_cases_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/global_avg_new_cases"
)

spike_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/spike_days"
)

peak_death_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/peak_global_death"
)

monthly_growth_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/monthly_death_growth"
)

print("\n===== TASK 6 ANALYTICS WRITTEN TO HDFS =====")

spark.stop()