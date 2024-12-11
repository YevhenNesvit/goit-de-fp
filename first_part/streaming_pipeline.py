import os

# Налаштування Spark з потрібними пакетами
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from configs import kafka_config, jdbc_url, jdbc_user, jdbc_password

# Налаштування Kafka і підключення до MySQL
# Створення Spark сесії
spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .appName("JDBCToKafka") \
    .getOrCreate()

# Налаштування Kafka
kafka_input_topic = "athlete_event_results"
kafka_output_topic = "aggregated_athlete_results_nesvit"

# 1. Читання та обробка даних із MySQL таблиць
# Читання даних з MySQL таблиці для біографії атлетів
athlete_bio_table = "athlete_bio"
athlete_bio_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable=athlete_bio_table,
    user=jdbc_user,
    password=jdbc_password) \
    .load()

# Фільтрація даних для атлетів, де зріст і вага є коректними числами
athlete_bio_cleaned_df = athlete_bio_df.filter(
    (col("height").isNotNull()) & (col("weight").isNotNull()) &
    (col("height").cast("float").isNotNull()) & (col("weight").cast("float").isNotNull())
)

# athlete_bio_cleaned_df.show()

# Читання даних з MySQL таблиці для результатів змагань
athlete_event_results_table = "athlete_event_results"
event_results_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable=athlete_event_results_table,
    user=jdbc_user,
    password=jdbc_password) \
    .load()

# event_results_df.show()

# Запис даних результатів змагань у Kafka топік
event_results_df.selectExpr("CAST(athlete_id AS STRING) as key", "to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
    .option("topic", kafka_input_topic) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';") \
    .save()

# kafka_read_df = spark.read.format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
#      .option("kafka.security.protocol", "SASL_PLAINTEXT") \
#     .option("kafka.sasl.mechanism", "PLAIN") \
#     .option("kafka.sasl.jaas.config",
#             'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
#     .option("subscribe", kafka_input_topic) \
#     .load()

# Перетворення даних з формату Kafka (ключ/значення) у формат JSON
# kafka_read_json_df = kafka_read_df.selectExpr("CAST(value AS STRING) as json").show(truncate=False)

# # 3. Читання даних з Kafka топіка
# # Схема для JSON даних
# schema = StructType([
#     StructField("athlete_id", StringType(), True),
#     StructField("event", StringType(), True),
#     StructField("medal", StringType(), True),
#     StructField("result", StringType(), True)
# ])

# # Читання даних з Kafka топіка
# kafka_df = spark.readStream.format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#     .option("subscribe", kafka_input_topic) \
#     .load()

# # Перетворення даних з формату Kafka (ключ/значення) у формат JSON
# kafka_json_df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")

# # 4. Обробка та агрегація даних
# # Приєднуємо результати змагань до біографічних даних атлетів за допомогою athlete_id
# joined_df = kafka_json_df.join(athlete_bio_cleaned_df, on="athlete_id", how="inner")

# # Обчислюємо середній зріст і вагу атлетів індивідуально для кожного виду спорту, типу медалі, статі, країни
# aggregated_df = joined_df.groupBy("sport", "medal", "gender", "country_noc") \
#     .agg(
#         avg("height").alias("avg_height"),
#         avg("weight").alias("avg_weight"),
#         current_timestamp().alias("timestamp")
#     )

# # 5. Запис результатів у Kafka і MySQL
# # Функція для обробки кожної партії даних
# def foreach_batch_function(batch_df, batch_id):
#     # Відправка збагачених даних до Kafka
#     batch_df.selectExpr("to_json(struct(*)) AS value") \
#         .write \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#         .option("topic", kafka_output_topic) \
#         .save()

#     # Збереження збагачених даних до MySQL
#     batch_df.write \
#         .format("jdbc") \
#         .option("url", jdbc_url) \
#         .option("driver", "com.mysql.cj.jdbc.Driver") \
#         .option("dbtable", "aggregated_results_nesvit") \
#         .option("user", jdbc_user) \
#         .option("password", jdbc_password) \
#         .mode("append") \
#         .save()

# # 6. Запуск потоку обробки даних
# # Налаштування потоку даних для обробки кожної партії
# aggregated_df.writeStream \
#     .foreachBatch(foreach_batch_function) \
#     .outputMode("update") \
#     .start() \
#     .awaitTermination()
