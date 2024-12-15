from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp


def process_silver_to_gold():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    # Read Silver tables
    athlete_bio = spark.read.parquet("second_part/silver/athlete_bio")
    athlete_event_results = spark.read.parquet("second_part/silver/athlete_event_results").drop("country_noc")

    # Join tables
    df = athlete_bio.join(athlete_event_results, on="athlete_id", how="inner")

    # Calculate averages
    result = df.groupBy("sport", "medal", "sex", "country_noc").agg(
        avg(col("weight").cast("float")).alias("avg_weight"),
        avg(col("height").cast("float")).alias("avg_height")
    )

    # Add timestamp
    result = result.withColumn("timestamp", current_timestamp())

    # Save to Gold
    result.write.mode("overwrite").parquet("second_part/gold/avg_stats")
    print("Data saved to gold/avg_stats")
    result.show(truncate=False)


process_silver_to_gold()
