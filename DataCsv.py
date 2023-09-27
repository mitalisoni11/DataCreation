from pyspark.sql import SparkSession
from pyspark.sql.types import *
from faker import Faker
import numpy as np
import os


def createCsvFile():
    fake = Faker(['en_US'])
    # Create a spark session
    spark = SparkSession.builder.appName("CreateParquetMap").getOrCreate()
    # Define the schema for the dataframe
    schema = StructType([
        StructField("Name", StringType()),
        StructField("id", IntegerType()),
        StructField("Subscribe", BooleanType()),
        StructField("DOB", DateType()),
        StructField("Income", FloatType()),
        StructField("Timestamp", TimestampType()),
    ])

    # Define the delimiter, escape character, and text qualifier
    delimiter = "|"
    escape_char = "\\"
    text_qualifier = "'"
    file_path = "/Users/sonmit01/Documents/Data/xyz.csv"

    df = []

    for i in range(50):
        Name = text_qualifier+fake.first_name()+text_qualifier
        id = fake.random_int(min=1000, max=3000)
        subscribe = fake.boolean()
        dob = fake.date_of_birth(minimum_age=18, maximum_age=50)
        income = abs(np.random.normal(18000, 500000))
        timestamp = fake.date_time()

        # df.append((Name,id,subscribe,contact_numbers,dob,income,timestamp,address))
        df.append((Name, id, subscribe, dob, income, timestamp))


    # Create a dataframe with the defined schema
    df = spark.createDataFrame(df, schema)

    df.coalesce(1).write.mode("overwrite").option("delimiter", delimiter).option("escape", escape_char).option("quote",text_qualifier).option("compression", "none").csv(file_path)
    # Stop the spark session
    spark.stop()

    with os.scandir(file_path) as files:
        for file in files:
            if file.name.startswith('part-00000'):
                print(file.name)
                os.rename(file, file_path+'/'+'test_data_2.csv')


if __name__ == '__main__':
    createCsvFile()