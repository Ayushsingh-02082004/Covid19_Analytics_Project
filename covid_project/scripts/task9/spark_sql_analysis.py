from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder \
    .appName("Spark-SQL-Analysis") \
    .getOrCreate()

# Read Dataset from HDFS

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs:///data/covid/raw/country_wise_latest.csv")

# Create Temporary View

df.createOrReplaceTempView("covid_data")

print("\n===== TEMP VIEW CREATED =====")

# 1. Top 10 Infection Countries

top10_sql = spark.sql("""
SELECT
    `Country/Region`,
    Confirmed
FROM covid_data
ORDER BY Confirmed DESC
LIMIT 10
""")

print("\n===== TOP 10 INFECTION COUNTRIES =====")

top10_sql.show()

# 2. Death Percentage Ranking

death_percentage_sql = spark.sql("""
SELECT
    `Country/Region`,
    Confirmed,
    Deaths,
    ROUND((Deaths / Confirmed) * 100, 2)
        AS Death_Percentage
FROM covid_data
ORDER BY Death_Percentage DESC
""")

print("\n===== DEATH PERCENTAGE RANKING =====")

death_percentage_sql.show(10)

# 3. Rolling 7-Day Average

day_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs:///data/covid/raw/day_wise.csv")

day_df.createOrReplaceTempView("day_data")

rolling_avg_sql = spark.sql("""
SELECT
    Date,
    `New cases`,
    ROUND(
        AVG(`New cases`) OVER (
            ORDER BY Date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS Rolling_7Day_Avg
FROM day_data
""")

print("\n===== ROLLING 7-DAY AVERAGE =====")

rolling_avg_sql.show(10)

# Compare Physical Plans

print("\n===== SQL PHYSICAL PLAN =====")

top10_sql.explain()

# DataFrame Version
df_plan = df.orderBy("Confirmed")

print("\n===== DATAFRAME PHYSICAL PLAN =====")

df_plan.explain()

# Write Outputs to HDFS

top10_sql.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/sql_top10_infection"
)

death_percentage_sql.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/sql_death_percentage"
)

rolling_avg_sql.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/sql_rolling_avg"
)

print("\n===== TASK 9 ANALYTICS WRITTEN TO HDFS =====")

spark.stop()