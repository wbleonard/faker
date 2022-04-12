import params
import sys
import time
from datetime import datetime
from pymongo import MongoClient
from faker import Faker
import argparse
from business_objects import *

# Process arguments
#parser = argparse.ArgumentParser(description='MongoDB Network Compression Test')
# parser.add_argument('-c', '--compressor', help="The compressor to use.",
#    choices=["snappy", 'zlib', 'zstd'])
#args = parser.parse_args()

print("\nGenerating Offers...")

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

groups = [
    {'Id': 'A', 'Label': 'Alpha'},
    {'Id': 'B', 'Label': 'Bravo'},
    {'Id': 'C', 'Label': 'Charlie'},
    {'Id': 'D', 'Label': 'Delta'},
    {'Id': 'E', 'Label': 'Echo'}]

while insert_count < records_to_insert:

    groups_value = []

    for x in range(fake.random_digit_not_null()):
        group = fake.random_element(groups)
        if group not in groups_value:   # Don't add the same group more than once
            groups_value.append(fake.random_element(groups))

    try:

        record = {
            "name": fake.name(),
            "email": fake.ascii_email(),
            "group": groups_value
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
