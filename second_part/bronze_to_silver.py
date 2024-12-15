import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\"\']', '', str(text))


clean_text_udf = udf(clean_text, StringType())


def process_bronze_to_silver(table_name):
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    # Read from Bronze
    df = spark.read.parquet(f"second_part/bronze/{table_name}")

    # Clean text columns
    for col_name in df.columns:
        if df.schema[col_name].dataType == StringType():
            df = df.withColumn(col_name, clean_text_udf(df[col_name]))

    # Deduplicate
    df = df.dropDuplicates()

    # Save to Silver
    output_path = f"second_part/silver/{table_name}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Data saved to {output_path}")
    df.show()


tables = ["athlete_bio", "athlete_event_results"]
for table in tables:
    process_bronze_to_silver(table)
