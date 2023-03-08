# %%
"""
This example puts/gets Customer and Order Portable objects into/from the
nw/customers and nw/orders maps. These classes are registered by default
in PadoGrid's Hazelcast clusters.
"""

from time import mktime
import time

from hazelcast.serialization.api import Portable

FACTORY_ID = 1

class Customer(Portable):
    CLASS_ID = 101

    def __init__(self, createdOn=None, updatedOn=None,
                 customerId=None, companyName=None, contactName=None, contactTitle=None,
                 address=None, city=None, region=None, postalCode=None, country=None,
                 phone=None, fax=None):
        self.createdOn = createdOn
        self.updatedOn = updatedOn
        self.customerId = customerId
        self.companyName = companyName
        self.contactName = contactName
        self.contactTitle = contactTitle
        self.address = address
        self.city = city
        self.region = region
        
        self.postalCode = postalCode
        self.country = country
        self.phone = phone
        self.fax = fax
    
    def read_portable(self, reader):
        val = reader.read_long("createdOn")
        if val == -1:
            self.createdOn = None
        else:
            self.createdOn = time.localtime(val/1000)
        val = reader.read_long("updatedOn")
        if val == -1:
            self.updatedOn = None
        else:
            self.updatedOn = time.localtime(val/1000)
        self.customerId = reader.read_string("customerId")
        self.companyName = reader.read_string("companyName")
        self.contactName = reader.read_string("contactName")
        self.contactTitle = reader.read_string("contactTitle")
        self.address = reader.read_string("address")
        self.city = reader.read_string("city")
        self.region = reader.read_string("region")
        self.postalCode = reader.read_string("postalCode")
        self.country = reader.read_string("country")
        self.phone = reader.read_string("phone")
        self.fax = reader.read_string("fax")

    def write_portable(self, writer):
        if self.createdOn == None: 
            writer.write_long("createdOn", -1)
        else:
            writer.write_long("createdOn", int(mktime(self.createdOn)*1000))
        if self.updatedOn == None: 
            writer.write_long("updatedOn", -1)
        else:
            writer.write_long("updatedOn", int(mktime(self.updatedOn)*1000))
        writer.write_string("customerId", self.customerId)
        writer.write_string("companyName", self.companyName)
        writer.write_string("contactName", self.contactName)
        writer.write_string("contactTitle", self.contactTitle)
        writer.write_string("address", self.address)
        writer.write_string("city", self.city)
        writer.write_string("region", self.region)
        writer.write_string("postalCode", self.postalCode)
        writer.write_string("country", self.country)
        writer.write_string("phone", self.phone)
        writer.write_string("fax", self.fax)

    def get_factory_id(self):
        return FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def __repr__(self):
        return "[createdOn=" + repr(self.createdOn) \
            + ", updatedOn=" + repr(self.updatedOn) \
            + ", customerId=" + repr(self.customerId) \
            + ", companyName=" + repr(self.companyName) \
            + ", contactName=" + repr(self.contactName) \
            + ", contactTitle=" + repr(self.contactTitle) \
            + ", address=" + repr(self.address) \
            + ", region=" + repr(self.region) \
            + ", postalCode=" + repr(self.postalCode) \
            + ", country=" + repr(self.country) \
            + ", phone=" + repr(self.phone) \
            + ", fax=" + repr(self.fax) + "]"
    
    def __str__(self):
        return "[createdOn=" + str(self.createdOn) \
            + ", updatedOn=" + str(self.updatedOn) \
            + ", customerId=" + str(self.customerId) \
            + ", companyName=" + str(self.companyName) \
            + ", contactName=" + str(self.contactName) \
            + ", contactTitle=" + str(self.contactTitle) \
            + ", address=" + str(self.address) \
            + ", region=" + str(self.region) \
            + ", postalCode=" + str(self.postalCode) \
            + ", country=" + str(self.country) \
            + ", phone=" + str(self.phone) \
            + ", fax=" + str(self.fax) + "]"
                                                                   
class Order(Portable):
    CLASS_ID = 109

    def __init__(self, createdOn=None, updatedOn=None,
                 orderId=None, customerId=None, employeeId=None, orderDate=None, requiredDate=None,
                 shippedDate=None, shipVia=None, freight=None, shipName=None, shipAddress=None,
                 shipCity=None, shipRegion=None, shipPostalCode=None, shipCountry=None):
        self.createdOn = createdOn
        self.updatedOn = updatedOn
        self.orderId = orderId
        self.customerId = customerId
        self.employeeId = employeeId
        self.orderDate = orderDate
        self.requiredDate = requiredDate
        self.shippedDate = shippedDate
        self.shipVia = shipVia
        self.freight = freight
        self.shipName = shipName
        self.shipAddress = shipAddress
        self.shipCity = shipCity
        self.shipRegion = shipRegion
        self.shipPostalCode = shipPostalCode
        self.shipCountry = shipCountry
        
    def read_portable(self, reader):
        val = reader.read_long("createdOn")
        if val == -1:
            self.createdOn = None
        else:
            self.createdOn = time.localtime(val/1000)
        val = reader.read_long("updatedOn")
        if val == -1:
            self.updatedOn = None
        else:
            self.updatedOn = time.localtime(val/1000)
        self.orderId = reader.read_string("orderId");
        self.customerId = reader.read_string("customerId")
        self.employeeId = reader.read_string("employeeId")                                    
        val = reader.read_long("orderDate")
        if val == -1:
            self.orderDate = None
        else:
            self.orderDate = time.localtime(val/1000)
        val = reader.read_long("requiredDate")
        if val == -1:
            self.requiredDate = None
        else:
            self.requiredDate = time.localtime(val/1000)
        val = reader.read_long("shippedDate")
        if val == -1:
            self.shippedDate = None
        else:
            self.shippedDate = time.localtime(val/1000)
        self.shipVia = reader.read_string("shipVia")
        self.freight = reader.read_double("freight")
        self.shipName = reader.read_string("shipName");
        self.shipAddress = reader.read_string("shipAddress")
        self.shipCity = reader.read_string("shipCity")
        self.shipRegion = reader.read_string("shipRegion")
        self.shipPostalCode = reader.read_string("shipPostalCode")
        self.shipCountry = reader.read_string("shipCountry")

    def write_portable(self, writer):
        if self.createdOn == None: 
            writer.write_long("createdOn", -1)
        else:
            writer.write_long("createdOn", int(mktime(self.createdOn)*1000))
        if self.updatedOn == None: 
            writer.write_long("updatedOn", -1)
        else:
            writer.write_long("updatedOn", int(mktime(self.updatedOn)*1000))
        writer.write_string("orderId", self.orderId);
        writer.write_string("customerId", self.customerId);
        writer.write_string("employeeId", self.employeeId);
        if self.orderDate == None: 
            writer.write_long("orderDate", -1)
        else:
            writer.write_long("orderDate", int(mktime(self.orderDate)*1000))
        if self.requiredDate == None: 
            writer.write_long("requiredDate", -1)
        else:
            writer.write_long("requiredDate", int(mktime(self.requiredDate)*1000))
        if self.shippedDate == None:
            writer.write_long("shippedDate", -1)
        else:
            writer.write_long("shippedDate", int(mktime(self.shippedDate)*1000))
        writer.write_string("shipVia", self.shipVia);
        writer.write_double("freight", self.freight);
        writer.write_string("shipName", self.shipName);
        writer.write_string("shipAddress", self.shipAddress);
        writer.write_string("shipCity", self.shipCity);
        writer.write_string("shipRegion", self.shipRegion);
        writer.write_string("shipPostalCode", self.shipPostalCode)
        writer.write_string("shipCountry", self.shipCountry)

    def get_factory_id(self):
        return FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID
    
    def __repr__(self):
        return "[createdOn=" + repr(self.createdOn) \
            + ", updatedOn=" + repr(self.updatedOn) \
            + ", orderId=" + repr(self.orderId) \
            + ", customerId=" + repr(self.customerId) \
            + ", employeeId=" + repr(self.employeeId) \
            + ", orderDate=" + repr(self.orderDate) \
            + ", requiredDate=" + repr(self.requiredDate) \
            + ", shippedDate=" + repr(self.shippedDate) \
            + ", shipVia=" + repr(self.shipVia) \
            + ", freight=" + repr(self.freight) \
            + ", shipName=" + repr(self.shipName) \
            + ", shipAddress=" + repr(self.shipAddress) \
            + ", shipCity=" + repr(self.shipCity) \
            + ", shipRegion=" + repr(self.shipRegion) \
            + ", shipPostalCode=" + repr(self.shipPostalCode) \
            + ", shipCountry=" + repr(self.shipCountry) + "]"
    
    def __str__(self):
        return "[createdOn=" + str(self.createdOn) \
            + ", updatedOn=" + str(self.updatedOn) \
            + ", orderId=" + str(self.orderId) \
            + ", customerId=" + str(self.customerId) \
            + ", employeeId=" + str(self.employeeId) \
            + ", orderDate=" + str(self.orderDate) \
            + ", requiredDate=" + str(self.requiredDate) \
            + ", shippedDate=" + str(self.shippedDate) \
            + ", shipVia=" + str(self.shipVia) \
            + ", freight=" + str(self.freight) \
            + ", shipName=" + str(self.shipName) \
            + ", shipAddress=" + str(self.shipAddress) \
            + ", shipCity=" + str(self.shipCity) \
            + ", shipRegion=" + str(self.shipRegion) \
            + ", shipPostalCode=" + str(self.shipPostalCode) \
            + ", shipCountry=" + str(self.shipCountry) + "]"