from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    round,
    desc
)

# Create Spark Session
spark = SparkSession.builder \
    .appName("COVID-Death-Analysis") \
    .getOrCreate()

# Read full_grouped.csv from HDFS
full_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs:///data/covid/raw/full_grouped.csv")


# Read worldometer_data.csv from HDFS
world_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs:///data/covid/raw/worldometer_data.csv")

# Daily Death Percentage Per Country

daily_death_df = full_df.withColumn(
    "Death_Percentage",
    round((col("Deaths") / col("Confirmed")) * 100, 2)
)

print("\n===== DAILY DEATH PERCENTAGE =====")

daily_death_df.select(
    "Date",
    "Country/Region",
    "Confirmed",
    "Deaths",
    "Death_Percentage"
).show(10)

# Global Daily Death Percentage

global_df = full_df.groupBy("Date").agg(
    sum("Confirmed").alias("Global_Confirmed"),
    sum("Deaths").alias("Global_Deaths")
)

global_df = global_df.withColumn(
    "Global_Death_Percentage",
    round(
        (col("Global_Deaths") / col("Global_Confirmed")) * 100,
        2
    )
)

print("\n===== GLOBAL DAILY DEATH PERCENTAGE =====")

global_df.show(10)

# Continent-wise Death Percentage

continent_df = full_df.groupBy("Country/Region").agg(
    sum("Confirmed").alias("Confirmed"),
    sum("Deaths").alias("Deaths")
)

joined_df = continent_df.join(
    world_df,
    continent_df["Country/Region"] == world_df["Country/Region"],
    "inner"
)

continent_result = joined_df.groupBy("Continent").agg(
    sum("Confirmed").alias("Total_Confirmed"),
    sum("Deaths").alias("Total_Deaths")
)

continent_result = continent_result.withColumn(
    "Continent_Death_Percentage",
    round(
        (col("Total_Deaths") / col("Total_Confirmed")) * 100,
        2
    )
)

print("\n===== CONTINENT-WISE DEATH PERCENTAGE =====")

continent_result.show()

# Country with Highest Death Percentage

highest_death_df = daily_death_df.orderBy(
    desc("Death_Percentage")
)

print("\n===== COUNTRY WITH HIGHEST DEATH PERCENTAGE =====")

highest_death_df.select(
    "Country/Region",
    "Death_Percentage"
).show(1)

# Top 10 Countries by Deaths Per Capita

per_capita_df = world_df.withColumn(
    "Deaths_Per_Capita",
    round(
        (col("TotalDeaths") / col("Population")) * 100,
        4
    )
)

top10_df = per_capita_df.orderBy(
    desc("Deaths_Per_Capita")
)

print("\n===== TOP 10 COUNTRIES BY DEATHS PER CAPITA =====")

top10_df.select(
    "Country/Region",
    "Population",
    "TotalDeaths",
    "Deaths_Per_Capita"
).show(10)

# Write Results to HDFS

daily_death_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/daily_death_percentage"
)

global_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/global_death_percentage"
)

continent_result.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/continent_death_percentage"
)

top10_df.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/top10_deaths_per_capita"
)

print("\n===== ALL ANALYTICS WRITTEN TO HDFS =====")

spark.stop()