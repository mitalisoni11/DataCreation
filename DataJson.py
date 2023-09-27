import random
from faker import Faker
import numpy as np
import json
import pandas as pd


def createFile():
    fake = Faker(['en_US'])
    boolean = [True, False]
    reg = ['Y', 'N']
    for i in range(100):
        dataFrame = {}
        subscribe = random.choice(boolean)  # boolean
        name = fake.first_name()  # String
        priority = fake.random_int(min=1000, max=3000)  # int64
        income = float(abs(np.random.normal(18000, 500000)))  # float64
        address = {"City": fake.city(), "State": fake.state()}  # struct
        countries_visited = [fake.country(), fake.country(), fake.country()]  # list
        date_of_birth = str(pd.to_datetime(fake.date_of_birth(minimum_age=18, maximum_age=50)))  # datetime
        null_key = None  # null
        email = 'abc@hotmail.com'
        id = random.randint(1,1000000)
        registered = random.choice(reg)

        values = [id, name, priority, subscribe, email, address, countries_visited, date_of_birth, null_key, registered,
                  income]
        keys = ["id", "name", "priority", "subscribe", "email", "address", "countries_visited", "date_of_birth",
                "null_key", "registered", "income"]

        for key in keys:
            for value in values:
                dataFrame[key] = value
                values.remove(value)
                break

        with open("/Users/sonmit01/Documents/Data/batch_json_5.json", 'a') as file1:
            json.dump(dataFrame, file1)
            file1.write("\n")


if __name__ == '__main__':
    createFile()
