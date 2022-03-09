import params
import sys
import time
from datetime import datetime
from pymongo import MongoClient
from faker import Faker
import argparse
from bson.decimal128 import Decimal128


# Process arguments
#parser = argparse.ArgumentParser(description='MongoDB Network Compression Test')
# parser.add_argument('-c', '--compressor', help="The compressor to use.",
#    choices=["snappy", 'zlib', 'zstd'])
#args = parser.parse_args()

print("\nGenerating records...")

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

records = []  # customers array for bulk insert

print("Records to insert: {},".format(records_to_insert))
print("Batch insert size: {},".format(batch_size))

while insert_count < records_to_insert:

    try:

        # Faker: https://faker.readthedocs.io/en/master/
        guid_text = '????????-????-????-????-????????'
        letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        both = '0123456789abcdefghijklmnopqrstuvwxyz'

        # "faa61186-6dfa-4326-9242-7a53e095928f"
        taxModel = fake.bothify(text=guid_text, letters=both)

        # Location (This is a tuple)
        location_on_land = fake.location_on_land(coords_only=False)
        latitude = Decimal128(str(location_on_land[0]))
        longitude = Decimal128(str(location_on_land[1]))
        locality = location_on_land[2]
        adminAreaLevelOne = location_on_land[3]

        record = {
            # "255179f5-c06a-439c-8885-e42118d0d8ce"
            "guuid": fake.bothify(text=guid_text, letters=both),
            "productId": fake.bothify(text='????', letters=both),
            "contentId": fake.bothify(text='????', letters=both),
            "productType": fake.bothify(text='????', letters=letters),
            "productSubType": "null",
            "complimentary": fake.boolean(chance_of_getting_true=2),
            "name": fake.word(),
            "facilityIds": [],
            "prices": [
                {
                    "guuid": fake.bothify(text=guid_text, letters=both),
                    "price": float(fake.random_digit_not_null()),
                    "currency": "USD",
                    "description": "Price description",
                    "layer": 0,
                    "validFrom": fake.past_datetime(start_date='-30d', tzinfo=None),
                    "validTo": fake.future_datetime(end_date='+30d', tzinfo=None),
                    "taxModel": taxModel
                },
                {
                    "guuid": fake.bothify(text=guid_text, letters=both),
                    "price": float(fake.random_digit_not_null()),
                    "currency": "USD",
                    "description": "Price description",
                    "layer": 1,
                    "validFrom": fake.past_datetime(start_date='-15d', tzinfo=None),
                    "validTo": fake.future_datetime(end_date='+45d', tzinfo=None),
                    "taxModel": taxModel
                },
                {
                    "guuid": fake.bothify(text=guid_text, letters=both),
                    "price": float(fake.random_digit_not_null()),
                    "currency": "USD",
                    "description": "Price description",
                    "layer": 2,
                    "validFrom": fake.past_datetime(start_date='-1d', tzinfo=None),
                    "validTo": fake.future_datetime(end_date='+60d', tzinfo=None),
                    "taxModel": "faa61186-6dfa-4326-9242-7a53e095928f"
                },
                {
                    "guuid": fake.bothify(text=guid_text, letters=both),
                    "price": float(fake.random_digit_not_null()),
                    "currency": "USD",
                    "description": "Price description",
                    "layer": 3,
                    "validTo": fake.future_datetime(end_date='+15d', tzinfo=None),
                    "validTo": fake.future_datetime(end_date='+75d', tzinfo=None),
                    "taxModel": taxModel
                },
            ],
            "experiences": [
                {
                    "id": fake.bothify(text='????', letters=both),
                    "name": fake.word(),
                    "park": fake.word(),
                    "locationIds": [{
                        "location": {"type": "Point", "coordinates": [longitude, latitude]},
                    },]
                },
            ],
            "available": fake.boolean(chance_of_getting_true=95),
            "travelCompany": fake.company()
        },

        # print(record)
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
