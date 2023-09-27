from pyspark.sql import SparkSession
from pyspark.sql.types import *
from faker import Faker
import numpy as np


def createFile():
    fake = Faker(['en_US'])
    # Create a spark session
    spark = SparkSession.builder.appName("CreateParquetMap").getOrCreate()
    # Define the schema for the dataframe
    schema = StructType([
        StructField("Name", StringType()),
        StructField("id", IntegerType()),
        StructField("Subscribe", BooleanType()),
        StructField("contact_numbers", MapType(StringType(), StringType())),
        StructField("DOB", DateType()),
        StructField("Income", FloatType()),
        StructField("Timestamp", TimestampType()),
        StructField("Address", StructType([StructField("City",StringType()),StructField("State",StringType())]))
    ])

    df = []

    for i in range(0):
        Name = fake.first_name()
        id = fake.random_int(min=1000, max=3000)
        subscribe = fake.boolean()
        contact_numbers = {"Home": "0265 3252079", "Office": "+1 324567890"}
        dob = fake.date_of_birth(minimum_age=18, maximum_age=50)
        income = abs(np.random.normal(18000, 500000))
        timestamp = fake.date_time()
        address = {"City": fake.city(), "State": fake.state()}

        df.append((Name,id,subscribe,contact_numbers,dob,income,timestamp,address))

    # Create a dataframe with the defined schema
    df = spark.createDataFrame(df, schema)

    # specify path where you want file to be saved and compression as gzip/snappy/uncompressed
    df.coalesce(1).write.mode("overwrite").option("compression", "uncompressed").parquet(
        "/Users/sonmit01/Documents/Data/alldata_header.parquet ")

    spark.stop()


if __name__ == '__main__':
    createFile()