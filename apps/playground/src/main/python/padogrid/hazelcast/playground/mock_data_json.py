import json
import time
import random
import uuid
from faker import Faker
from  hazelcast.core import HazelcastJsonValue

fake = Faker()
fake_customer = Faker()
fake_order = Faker()
customerId_prefix = "c"
orderId_prefix = "c"
default_key_type = 'random'
default_key_range = 100_000_000
day_in_msec = 24*60*60*1000

def create_customer_json(key=None, er_key=None, key_type=default_key_type, key_range=default_key_range):
    customer, key = create_customer(key=key, er_key=er_key, key_range=key_range)
    return HazelcastJsonValue(json.dumps(customer)), key

def create_customer(key=None, er_key=None, key_type=default_key_type, key_range=default_key_range):
    # num=random.randint(1,1000)
    if key == None:
        if key_type == 'uuid':
            # random uuid
            key = str(uuid.uuid4())
        else:
            num = random.randint(0, key_range-1)
            #num = fake_customer.unique.random_int()
            key = customerId_prefix + str(num).zfill(8)
    current_time_in_msec = time.time() * 1000
    customer = {
        "createdOn": current_time_in_msec,
        "updatedOn": current_time_in_msec,
        "address": fake.street_address(),
        "city": fake.city(), 
        "companyName" : fake.company(),
        "contactName": fake.name(), 
        "contactTitle": fake.job(),
        "country": fake.country(), 
        "customerId": key, 
        "fax": fake.phone_number(),
        "phone": fake.phone_number(),
        "postalCode": fake.postcode(),
        "region": fake.state()
    }
    return customer, key

def create_order_json(key=None, er_key=None, key_type=default_key_type, key_range=default_key_range):
    order, key = create_order(key=key, er_key=er_key)
    return HazelcastJsonValue(json.dumps(order)), key

def create_order(key=None, er_key=None, key_type=default_key_type, key_range=default_key_range):
    if key == None:
        if key_type == 'uuid':
            # random uuid
            key = str(uuid.uuid4())
        else:
            num = random.randint(0, key_range-1)
            #num = fake_order.unique.random_int()
            key = orderId_prefix + str(num).zfill(8)
    if er_key == None:
        num = random.randint(0, key_range-1)
        #num = fake_customer.unique.random_int()
        er_key = str(num)
        er_key = customerId_prefix + er_key.zfill(8)
    freight=round(random.random() * 100, 2)
    shipVia = str (int (random.random() * 7))
    current_time_in_msec = time.time() * 1000
    order = {
        "createdOn": current_time_in_msec,
        "updatedOn": current_time_in_msec,
        "customerId": key,
        "employeeId": fake.ssn(),
        "freight": freight,
        "orderDate": current_time_in_msec,
        "orderId": er_key,
        "requiredDate": current_time_in_msec + day_in_msec*5,
        "shipAddress": fake.street_address(),
        "shipCity": fake.city(),
        "shipCountry": fake.country(),
        "shipName": fake.company(),
        "shipPostalCode": fake.postcode(),
        "shipRegion": fake.state(),
        "shipVia": shipVia,
        "shippedDate": current_time_in_msec + day_in_msec,
    }
    return order, key