import time
import random
import uuid
from faker import Faker
from  hazelcast.core import HazelcastJsonValue

from padogrid.hazelcast.playground.nw_portable import Customer
from padogrid.hazelcast.playground.nw_portable import Order

fake = Faker()
fake_customer = Faker()
fake_order = Faker()
customerId_prefix = 'c'
orderId_prefix = 'o'
default_key_type = 'random'
default_key_range = 100_000_000

def create_customer(key=None, er_key=None, key_type=default_key_type, key_range=default_key_range):
    '''Returns a new Customer object. er_key is ignored.

    Args:
        key: customerID. If not specified then one is created.
        er_key: Ignored.
        key_type: Key type. Valid values are 'random', 'uuid'. All others default to 'random'.
        key_range: If key_type is 'random' then this value sets the maximum integer value of the key.
                   Default: 100_000_000
    '''
    if key == None:
        if key_type == 'uuid':
            # random uuid
            key = str(uuid.uuid4())
        else:
            num = random.randint(0, key_range-1)
            #num = fake_customer.unique.random_int()
            key = customerId_prefix + str(num).zfill(8)
    customer = Customer(createdOn=time.localtime(), updatedOn=time.localtime(),
                    address=fake.street_address(), 
                    city=fake.city(), companyName=fake.company(),
                    contactName=fake.name(), contactTitle=fake.job(),
                    country=fake.country(), customerId=key, fax=fake.phone_number(),
                    phone=fake.phone_number(), postalCode=fake.postcode(), region=fake.state())
    return customer, key

def create_order(key=None, er_key=None, key_type=default_key_type, key_range=default_key_range):
    '''Returns a new Order object.

    Args:
        key: order ID. If not specified then one is created.
        er_key: customer ID. If not specified then one is created.
        key_type: Key type. Valid values are 'random', 'uuid'. All others default to 'random'.
        key_range: If key_type is 'random' then this value sets the maximum integer value of the key.
                   Default: 100_000_000
    '''
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
        er_key = customerId_prefix + str(num).zfill(8)
    freight=round(random.random() * 100, 2)
    shipVia = str (int (random.random() * 7))
    order = Order(createdOn=time.localtime(), updatedOn=time.localtime(),
                customerId=er_key, employeeId=fake.ssn(), freight=freight,
                orderDate=time.localtime(), orderId=key, requiredDate=time.localtime(), 
                shipAddress=fake.street_address(), 
                shipCity=fake.city(), shipCountry=fake.country(),
                shipName=fake.company(), shipPostalCode=fake.postcode(), shipRegion=fake.state(),
                shipVia=shipVia, shippedDate=time.localtime())
    return order, key