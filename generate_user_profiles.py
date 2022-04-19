from tokenize import group
import params
import sys
import time
from datetime import datetime
import pymongo
from pymongo import MongoClient
from faker import Faker
import argparse
import json
from business_objects import *


def get_collection(collection):

    # Establish connection to MongoDB
    # https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html?highlight=compression#pymongo.mongo_client.MongoClient
    client = MongoClient(params.target_conn_string,
                         compressors=params.compressor)
    db = client[params.target_database]
    return db[collection]


def get_group_document(groupId):
    #collection = get_collection(params.group_collection)
    return groups_collection.find_one({'Id': groupId}, {'_id': 0})


def process_records(groups_collection, target_collection):

    print("\nGenerating Records...")
    print("Now:", datetime.now(), "\n")

    if params.drop_collection:
        target_collection.drop()

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
        {'Id': 'E', 'Label': 'Echo'},
        {'Id': 'F', 'Label': 'Foxtrot'},
        {'Id': 'G', 'Label': 'Golf'}]


    groups_collection.drop()
    groups_collection.insert_many(groups)

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

                target_collection.insert_many(records)
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


def group_size(collection, group):
    count = collection.count_documents({'group.Id': group})
    return count



def commit_with_retry(session):
    while True:
        try:
            # Commit uses write concern set at transaction start.
            session.commit_transaction()
            print("Transaction committed.")
            break
        except (pymongo.errors.ConnectionFailure, pymongo.errors.OperationFailure) as exc:
            # Can retry commit
            if exc.has_error_label("UnknownTransactionCommitResult"):
                print("UnknownTransactionCommitResult, retrying "
                      "commit operation ...")
                continue
            else:
                print("Error during commit ...")
                raise

# For millions of documents this process can exceed the 60 second timeout, which can only be extended via support
# https://www.mongodb.com/docs/manual/reference/parameters/#mongodb-parameter-param.transactionLifetimeLimitSeconds
def delete_group_txn(session, groups_collection, users_collection, group):
    group_document = get_group_document(group)

    print('Deleting {:,} members'.format(count), 'from group', group)
    print(group_document)

    print("Now:", datetime.now(), "\n")
    t_start = time.time()

    while True:
        try:
            with session.start_transaction():

                # See Remove Items from an Array of Documents
                # https://www.mongodb.com/docs/manual/reference/operator/update/pull/#remove-items-from-an-array-of-documents
                result = users_collection.update_many({'group.Id': group}, {'$pull': {'group': group_document}}, upsert=False, session=session)
                print('{:,} user profiles updated in'.format(result.modified_count), str(round(time.time()-t_start, 0)), 'seconds')

                result = groups_collection.delete_one({'Id': group}, session=session)
                print('Group', group, 'deleted from', params.group_collection, 'collection')

                commit_with_retry(session)
            break
        except (pymongo.errors.ConnectionFailure, pymongo.errors.OperationFailure) as exc:
            # If transient error, retry the whole transaction
            if exc.has_error_label("TransientTransactionError"):
                print("TransientTransactionError, retrying transaction ...")
                continue
            else:
                raise            

def delete_group(groups_collection, users_collection, group):
    group_document = get_group_document(group)

    print('Deleting {:,} members'.format(count), 'from group', group)
    print(group_document)

    print("Now:", datetime.now(), "\n")
    t_start = time.time()

    try:

        # See Remove Items from an Array of Documents
        # https://www.mongodb.com/docs/manual/reference/operator/update/pull/#remove-items-from-an-array-of-documents
        result = users_collection.update_many({'group.Id': group}, {'$pull': {'group': group_document}})
        print('{:,} user profiles updated in'.format(result.modified_count), str(round(time.time()-t_start, 0)), 'seconds')

        result = groups_collection.delete_one({'Id': group})
        print('Group', group, 'deleted from', params.group_collection)

    except Exception as e:
        print("Error deleting group ...")
        print(e)
        sys.exit(0)


def add_group(groups_collection, users_collection, group_document):

    print('Adding', group_document, '...')

    groups_collection.insert_one(group_document)

    # Using existing group F as a cheap way to add a new group.
    users_collection.update_many(
        {'group.Id': 'F'}, {'$push': {'group': group_document}})


if __name__ == "__main__":

    # Process arguments
    parser = argparse.ArgumentParser(description='MongoDB Large Transaction Test')
    parser.add_argument('-t', '--task', help="The task to run",
                    choices=['load', 'group_size', 'add_group', 'delete_group', 'delete_group_txn'])
    parser.add_argument('-g', '--group', help="The group Id")
    args = parser.parse_args()

    # Establish connection to MongoDB
    # https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html?highlight=compression#pymongo.mongo_client.MongoClient
    client = MongoClient(params.target_conn_string, compressors=params.compressor)
    database = client[params.target_database]
    user_profiles_collection = database[params.target_collection]
    groups_collection = database[params.group_collection]

    # python3 generate_user_profiles.py -t load    
    if (args.task == 'load'):
        process_records(groups_collection, user_profiles_collection)

    # Return the count of group members
    # python3 generate_user_profiles.py -t group_size -g A
    elif (args.task == 'group_size'):
        count = group_size(user_profiles_collection, args.group)

        print('Group', args.group, 'has {:,} members'.format(count))

    # Add Group
    # The -g arg should be the full group document. E.g., '{"Id": "H", "Label": "Hotel"}'
    # python3 generate_user_profiles.py -t add_group -g '{"Id": "H", "Label": "Hotel"}'
    elif (args.task == 'add_group'):
        group = json.loads(args.group)
        add_group(groups_collection, user_profiles_collection, group)

    # Delete Group
    # The -g arg should be the groupId. E.g. E
    # python3 generate_user_profiles.py -t delete_group -g E
    elif (args.task == 'delete_group'):
        count = group_size(user_profiles_collection, args.group)

        delete_group(groups_collection, user_profiles_collection, args.group)
    
    # The -g arg should be the groupId. E.g. E
    # python3 generate_user_profiles.py -t delete_group_txn -g E
    elif (args.task == 'delete_group_txn'):
        count = group_size(user_profiles_collection, args.group)

        # Start a client session.
        with client.start_session() as session:
            # Use with_transaction to start a transaction, execute the callback, and commit (or abort on error).
            delete_group_txn(session, groups_collection, user_profiles_collection, args.group)

    else:
        print("usage: generate_user_profiles.py [-h] [-t {load,group_size,delete_group}]")
