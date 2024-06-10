from pyspark.sql import SparkSession
import json
import os
import pandas
import logging
import DjangoSetup
from core.models import db_DEVICE, db_MANUFACTURER
from core.Drivers.AirVeda import AirVeda

logger=logging.getLogger('workspace')


spark = SparkSession.builder \
    .appName("Django Spark Integration") \
    .getOrCreate()

try:
    all_device = db_DEVICE.getAllDevice()
    data = list(all_device)

    def fetchDeviceDict(data):
        device_Dict = {}
        for device in data:
            if device.manufacturer_id in device_Dict.keys():
                device_Dict[device.manufacturer_id].append(device)
            else:
                device_Dict[device.manufacturer_id] = []
                device_Dict[device.manufacturer_id].append(device)
        return device_Dict

    device_Dict = fetchDeviceDict(data)

    from core.Drivers.DriverList import driverList as drivers
    print(drivers)
    print(device_Dict)
    for i in device_Dict.keys():
        try:
            logger.info(f"Processing devices for manufacturer: {i.name}")
            print(i.name)
            obj = drivers[i.name](manufacturer_obj=i)
            print(obj)
            da = obj.fetch(deviceObj=device_Dict[i])
            print(da)
            logger.info(f"Fetched data: {da}")

            obj.standardization_df()
            logger.info(f"Standardized DataFrame: {obj._df_all}")
            logger.info(f"Missing data dictionary: {obj._missing_data_dict}")
            print(obj._cal_df)
            obj.store_std_data()
            logger.info(f"Stored standardized data for manufacturer: {i.name}")

        except Exception as e:
            logger.error(f"Error processing devices for manufacturer {i.name}: {e}")
            continue

except Exception as e:
    logger.error(f"Error in main processing: {e}")
    raise
