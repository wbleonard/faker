import params
import sys
import time
from datetime import datetime
from pymongo import MongoClient
from faker import Faker
import argparse
import random

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

records = []  # records array for bulk insert

print ("Records to insert: {}".format(records_to_insert))
print ("Batch insert size: {}".format(batch_size))

while insert_count < records_to_insert:

    try:

        record = {}
        choice = random.randint(0,2)      

        if (choice == 0):

            record = {
                "hello": fake.random_digit(),
                "test": None
            }
        
        elif (choice == 1):

            record = {
                "hello": fake.random_digit(),
                "test": fake.random_digit()
            }
        
        else:

            record = {
                "hello": fake.random_digit(),
            }

        records.append(record)

        insert_count += 1
               
         # If the modulus falls in the range of the record size, insert the batch.
        if(insert_count % batch_size == 0):

            collection.insert_many(records)
            records = []

            # Print performance stats
            duration = time.time()-t_start
            print('{:.0f} records inserted'.format(insert_count), 'at {:.1f} records/second'.format(insert_count/duration))

    except KeyboardInterrupt:
        print
        sys.exit(0)

    except Exception as e:
        print('\n********\nConnection Problem:')
        print(e)
        print('********')
        sys.exit(0)


print("\n", insert_count, 'records inserted in', str(round(time.time()-t_start, 0)), 'seconds')

