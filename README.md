# Faker
Sample Data Generation Examples Using Faker


## Setup

### Data Generator
[Faker](https://faker.readthedocs.io/en/master/) is a Python package that is used to generate fake data for this test.

```pip3 install faker ```
### Compression Library
Snappy compression in Python requires the `python-snappy` package.

```pip3 install python-snappy```




### Client Configuration

Edit [params.py](params.py) and at a minimum, set your connection string. Other tunables include the number of records to insert (default 30,000) and the batch insert size (jdefault 10,000):

``` PYTHON
drop_collection = False   # Drop collection on run
records_to_insert = 30000
batch_size = 10000       # Batch size of bulk insert
```

## Execution

```zsh
python3 generate_offers.py
python3 generate_offers.py

Generating Offers...
Now: 2021-10-01 10:07:14.226110 

Records to insert: 30000
Batch insert size: 10000
10000 records inserted at 1233.6 records/second
20000 records inserted at 1282.0 records/second
30000 records inserted at 1297.0 records/second

 30000 records inserted in 23.0 seconds
```

