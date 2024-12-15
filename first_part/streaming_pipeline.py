import os

# Налаштування Spark з потрібними пакетами
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from configs import kafka_config, jdbc_url, jdbc_user, jdbc_password

# Налаштування Kafka і підключення до MySQL
# Створення Spark сесії
spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .appName("JDBCToKafka") \
    .getOrCreate()

# Налаштування Kafka
kafka_input_topic = "athlete_event_results_nesvit"
kafka_output_topic = "aggregated_athlete_results_nesvit"

# 1. Читання даних з MySQL таблиці для біографії атлетів
athlete_bio_table = "athlete_bio"
athlete_bio_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable=athlete_bio_table,
    user=jdbc_user,
    password=jdbc_password).load()

# 2. Фільтрація некоректних даних
athlete_bio_cleaned_df = athlete_bio_df.filter(
    (col("height").isNotNull()) & (col("weight").isNotNull()) &
    (col("height").cast("float").isNotNull()) & (col("weight").cast("float").isNotNull())
)

# 3. Читання даних з MySQL, запис і читання в/з кафка топіку таблиці для результатів змагань
athlete_event_results_table = "athlete_event_results"
event_results_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable=athlete_event_results_table,
    user=jdbc_user,
    password=jdbc_password).load()

# Запис даних результатів змагань у Kafka топік
event_results_df.selectExpr("CAST(athlete_id AS STRING) as key", "to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
    .option("topic", kafka_input_topic) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_config['username']}' \
            password='{kafka_config['password']}';") \
    .save()

# Читання даних з Kafka топіка
schema = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", IntegerType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", StringType(), True)
])

kafka_stream_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
    .option("subscribe", kafka_input_topic) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "300") \
    .load()

# Розпаковка JSON даних
kafka_json_df = kafka_stream_df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*").drop("country_noc")

# 4. Приєднання біографічних даних та результатів змагань
joined_stream_df = kafka_json_df.join(athlete_bio_cleaned_df, on="athlete_id", how="inner")

# 5. Агрегація даних
aggregated_stream_df = joined_stream_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp")
)


# Функція для запису даних у Kafka та MySQL
def foreach_batch_function(batch_df, batch_id):
    batch_df.show(truncate=False)
    # Запис у Kafka
    batch_df.selectExpr("to_json(struct(*)) AS value").write.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
        .option("topic", kafka_output_topic) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config",
                f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_config['username']}' \
                password='{kafka_config['password']}';") \
        .save()

    # Запис у MySQL
    batch_df.write.format("jdbc").options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="aggregated_results_nesvit",
        user=jdbc_user,
        password=jdbc_password).mode("append").save()


# 6. Запуск потоку
aggregated_stream_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .start() \
    .awaitTermination()
