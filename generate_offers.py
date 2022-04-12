import params
import sys
import time
from datetime import datetime
from pymongo import MongoClient
from faker import Faker
import argparse
from decimal import Decimal
from bson.decimal128 import Decimal128


def generateStoreLocations(storeName):

    stores = [0, 1, 2, 3, 4, 5, 6, 7]
    store_locations = []
    store = 0
    number_of_stores = fake.random_element(stores);

    while store <= number_of_stores:

        # Location (This is a tuple)
        location_on_land = fake.location_on_land(coords_only=False)
        latitude = Decimal128(str(location_on_land[0]))
        longitude = Decimal128(str(location_on_land[1]))
        locality = location_on_land[2]
        adminAreaLevelOne = location_on_land[3]

        storeLocation = {
            "storeName": storeName + fake.numerify(text='%%%%%%%%'),
            "address1": fake.street_address(),
            "address2": None,
            "locality": locality,
            "adminAreaLevelOne": adminAreaLevelOne,
            "postalCode": fake.postcode(),
            "location": {"type": "Point", "coordinates": [longitude, latitude]},
            "merchantIds": [{
                        "type": "VISA_MASTERCARD",
                        "value": "510159890101074"
            },]
        }
        
        store_locations.append(storeLocation)
        store = store + 1

    return store_locations


    return storeLocations


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

offers = []  # customers array for bulk insert

print("Records to insert: {},".format(records_to_insert))
print("Batch insert size: {},".format(batch_size))

while insert_count < records_to_insert:

    try:

        # Faker Providers: https://faker.readthedocs.io/en/master/providers.html

        # Location (This is a tuple)
        location_on_land = fake.location_on_land(coords_only=False)
        locality = location_on_land[2]
        adminAreaLevelOne = location_on_land[3]

        # Percent discount
        discount_percent = float(fake.random_digit_not_null())/100
        discount = str(discount_percent * 100)

        # Offer categories
        all_categories = ["RESTAURANT", "PIZZA", "APPAREL_ACCESSORIES", "FOOTWEAR", "ENTERTAINMENT", "MOVIES", "CABLE OR SATALELLITE", "STEAK", "APPAREL", "WOMEN'S CLOTHING", "WINE", "SUNGLASSES", "PHOTO PRINTING",
                          "MEDICAL", "ORAL CARE", "SOFTWERE", "ANTI-VIRUS", "FURNITURE", "MATTRESSES", "WINDOW TREATMENTS", "JEWELERY", "COSMETICS", "FITNESS", "EYE CARE", "EYE WEAR", "DELI", "MAGAZINE", "NEWS PAPER", "NUTRITION", "GROCERY"]
        advertiserCategory = fake.random_elements(
            elements=all_categories, length=2, unique=True)

        company = fake.company()

        offer = {
            "versionId": fake.numerify(text='%%%%%%%%'),
            "uniqueAggregatorId": "ABC",
            "uniquePartnerId": "acme-rewards",
            "targeting": None,
            "offerFlow": {
                "state": None
            },
            "offerDetails": {
                "type": 5,
                "startDate": fake.date(),
                "endDate": "2021-12-31",
                "relativeExpirationPeriodFromActivation": None,
                "minTranAmount": 0.000000,
                "maxTranAmount": None,
                "maxRewardAmount": None,
                "offerUniqueId": fake.numerify(text='%%%%%% '),
                "offerDiscountType": "PERCENT",
                "offerChannel": [
                    "INSTORE"
                ],
                "passiveOfferAmount": 0.000000,
                "offerDiscountAmount": discount_percent,
                "activationType": "CLICK_TO_ACTIVATE",
                "offerAmountTiers": [
                ],
                "offerText": {
                    "preMessageText": discount + "% back on your next purchase",
                    "postMessageText": "Earn" + discount + " % cash back on every purchase at " + company + ". Rewards can be earned on every purchase until a maximum of $2,000 in combined total spend each month is reached. <b>Purchase must be made directly with the merchant in store.</b> Payment must be made on or before expiration date. Offer not valid on third-party delivery services. <br/><br/>Offer valid only at: <br/>123 Main Street, " + locality + ", " + adminAreaLevelOne + " 08081",
                    "thankYouMessageText": "Thank you for choosing " + company + "!"
                },
            },
            "cardholderRewardCaps": [
                {
                    "type": "REDEEMED_TRANSACTION_AMOUNT",
                    "interval": "MONTH",
                    "value": 2000.000000
                },
            ],
            "exclusions": {
                "exclusionDayOfWeek": None,
                "exclusionStartTime": None,
                "exclusionEndTime": None,
                "exclusionStartDate": None,
                "exclusionEndDate": None,
                "exclusionDates": [
                ]
            },
            "merchant": {
                "advertiserId": fake.random_number(digits=10, fix_len=True),
                "advertiserName": company,
                "advertiserWebsite": None,
                "advertiserCategory": advertiserCategory,
                "advertiserAlias": None
            },
            "merchantIds": [{
                "type": "VISA_MASTERCARD",
                "value": "510159890101074"
            },],
            "images": [{
                "description": "merchantLogoSmall",
                "url": "https://image-content.s3.amazonaws.com/bamd/1000041539-746283-logo-1619036979041.png",
                "resolution": "128.128",
                "altText": company
            }, {
                "description": "merchantLogoMedium",
                "url": "https://image-content.s3.amazonaws.com/bamd/1000041539-746283-logo-1619036979041.png",
                "resolution": "128.128",
                "altText": company
            }, {
                "description": "merchantLogoLarge",
                "url": "https://image-content.s3.amazonaws.com/bamd/1000041539-746283-logo-1619036979041.png",
                "resolution": "128.128",
                "altText": company
            },],
            "storeLocations": generateStoreLocations(company),
            "budget": None,
            "alerts": [],
            "matchFormula": "R1",
            "matchRules": [{
                "id": "R1",
                "value": "merchantId | In | 510159890101074"
            },]
        } 
        # print(offer)
        offers.append(offer)

        insert_count += 1

        # If the modulus falls in the range of the record size, insert the batch.
        if(insert_count % batch_size == 0):

            collection.insert_many(offers)
            offers = []

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