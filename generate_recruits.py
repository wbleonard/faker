import params
import sys
import time
from datetime import datetime
from pymongo import MongoClient
from faker import Faker
import argparse
from business_objects import *
import json
from bson.decimal128 import Decimal128


print("\nGenerating Recruits...")

# Establish connection to MongoDB

# https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html?highlight=compression#pymongo.mongo_client.MongoClient
client = MongoClient(params.target_conn_string, compressors=params.compressor)

print("Now:", datetime.now(), "\n")

db = client[params.target_database]
collection = db[params.target_collection]

if params.drop_collection:
    collection.drop()

fake = Faker()
records_to_insert = params.records_to_insert
insert_count = 0  # Count the records inserted.
batch_size = params.batch_size
t_start = time.time()

records = []  # array for bulk insert

print("Records to insert: {},".format(records_to_insert))
print("Batch insert size: {},".format(batch_size))

skills = [
    "Creativity",
    "Interpersonal Skills",
    "Critical Thinking",
    "Problem Solving",
    "Public Speaking",
    "Customer Service Skills",
    "Teamwork Skills",
    "Communication",
    "Collaboration",
    "Accounting",
    "Active Listening",
    "Adaptability",
    "Negotiation",
    "Conflict Resolution",
    "Empathy",
    "Customer Service",
    "Decision Making",
    "Management",
    "Leadership skills",
    "Organization",
    "Language skills",
    "Administrative skills"]

degrees = [
    "Accounting",
    "Aerospace Engineering",
    "Agriculture",
    "Anthropology",
    "Architecture",
    "Art History",
    "Biology",
    "Business Management",
    "Chemical Engineering",
    "Chemistry",
    "Civil Engineering",
    "Communications",
    "Computer Engineering",
    "Computer Science",
    "Construction",
    "Criminal Justice",
    "Drama",
    "Economics",
    "Education",
    "Electrical Engineering",
    "English",
    "Film",
    "Finance",
    "Forestry",
    "Geography",
    "Geology",
    "Graphic Design",
    "Health Care Administration",
    "History",
    "Hospitality & Tourism",
    "Industrial Engineering",
    "Information Technology (IT)",
    "Interior Design",
    "International Relations",
    "Journalism",
    "Management Information Systems (MIS)",
    "Marketing",
    "Math",
    "Mechanical Engineering",
    "Music",
    "Nursing",
    "Nutrition",
    "Philosophy",
    "Physician Assistant",
    "Physics",
    "Political Science",
    "Psychology",
    "Religion",
    "Sociology",
    "Spanish"
]


def load_universities():
    universities = []
    with open("universities2.json") as f:
        data = json.load(f)
        for university in data["schools"]:
            university["degree"] = fake.random_element(degrees)
            university["gpa"] = (fake.random_int(min=0, max=40))/10
            universities.append(university)
    return universities


universities = load_universities()

while insert_count < records_to_insert:

    try:
        # Location (This is a tuple)
        location_on_land = fake.location_on_land(coords_only=False)
        latitude = Decimal128(str(location_on_land[0]))
        longitude = Decimal128(str(location_on_land[1]))
        locality = location_on_land[2]
        adminAreaLevelOne = location_on_land[3]

        record = {
            "name": fake.name(),
            "email": fake.ascii_email(),
            "phone_number": fake.phone_number(),
            "job": fake.job(),
            "skills": fake.random_choices(skills),
            "university": fake.random_element(universities),
            "address": {
                "street": fake.street_address(),
                #"city": fake.city(),
                "city": locality,
                "country": adminAreaLevelOne,
                "postcode": fake.postcode(),
                "location": {"type": "Point", "coordinates": [longitude, latitude]},
            },
            "ssn": fake.ssn(),
            "company": {
                "name": fake.company(),
                "catch_phrase": fake.catch_phrase(),
                "bs": fake.bs()
            },
            "notes": fake.text()
        }

        records.append(record)

        insert_count += 1

        # If the modulus falls in the range of the record size, insert the batch.
        if(insert_count % batch_size == 0):

            collection.insert_many(records)
            records = []

            # Print performance stats
            duration = time.time()-t_start
            print('{:.0f}, records inserted'.format(insert_count),
                  'at {:.1f}, records/second'.format(insert_count/duration))

    except KeyboardInterrupt:
        print
        sys.exit(0)

    except Exception as e:
        print('\n********\nConnection Problem:')
        print(e)
        print('********')
        sys.exit(0)


print("\n", insert_count, 'records inserted in',
      str(round(time.time()-t_start, 0)), 'seconds')
