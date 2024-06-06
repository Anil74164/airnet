from pyspark.sql import SparkSession

import json
import os
import pandas
import DjangoSetup
 
from core.models import db_DEVICE,db_MANUFACTURER

# Initialize Spark session
 
spark = SparkSession.builder \
        .appName("Django Spark Integration") \
        .getOrCreate()
 

 
all_device = db_DEVICE.getAllDevice()
data = list(all_device)


 
def fetchDeviceDict(data):
    device_Dict={} 
    for device in data:
        if device.manufacturer_id in device_Dict.keys():
            device_Dict[device.manufacturer_id ].append(device)
        else:
            device_Dict[device.manufacturer_id]=[]
            device_Dict[device.manufacturer_id ].append(device)
    return device_Dict     

device_Dict=fetchDeviceDict(data)  
    
from core.Drivers.DriverList import driverList as drivers


all_data = {}
for i in device_Dict.keys():
    
    obj = drivers[i.name](manufacturer_obj=i)
    da=obj.fetch(device_Dict[i])
    print(da)
    
    

    

