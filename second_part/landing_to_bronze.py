import requests
from pyspark.sql import SparkSession


def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open the local file in write-binary mode and write the content of the response to it
        with open(local_file_path + ".csv", 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")


def save_to_bronze(table_name):
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    # Read CSV file
    df = spark.read.csv(f"{table_name}.csv", header=True, inferSchema=True)

    # Save as Parquet
    output_path = f"second_part/bronze/{table_name}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Data saved to {output_path}")
    df.show()


tables = ["athlete_bio", "athlete_event_results"]
for table in tables:
    download_data(table)
    save_to_bronze(table)
