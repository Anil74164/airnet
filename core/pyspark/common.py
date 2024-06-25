from pyspark.sql import SparkSession
import json
import os
import pandas
import DjangoSetup
from core.models import db_DEVICE
import argparse
from datetime import datetime
import pytz
import logging
 
logger=logging.getLogger('workspace')
 
def spark_init():
    spark = SparkSession.builder \
        .appName("Django Spark Integration") \
        .getOrCreate()
    return spark
 
def get_options():
    parser = argparse.ArgumentParser(description = 'Airnet Data Fetch DAG')
    parser.add_argument('-s', '--start', default=None)
    parser.add_argument('-e', '--end', default=None)
    parser.add_argument('-d', '--device', default=None)
    parser.add_argument('-p', '--pollutant', default = None )
    args = parser.parse_args()
    # We get values like args.start, args.end, args.device, args.pollutant
    
    ist_timezone = pytz.timezone('Asia/Kolkata')
    if args.start!=None:
        try:
            args.start = datetime.strptime(args.start, "%Y%m%d%H%M%S")
            
            args.start=args.start.astimezone(ist_timezone)
            
            print(type(args.start))
        except Exception as e:
            logger.error("Invalid date format:"+args.start)
    if args.end:
        try:
            
            args.end = datetime.strptime(args.end, "%Y%m%d%H%M%S")
            args.end=args.end.astimezone(ist_timezone)
        except Exception as e:
            logger.error("Invalid date format:"+args.end)
    
    return args
 
 
def get_all_devices():
    all_device = db_DEVICE.get_active_devices()
    data = list(all_device)
    return data
 
def fetchDeviceDict(data):
    device_Dict = {}
    for device in data:
        if device.manufacturer_id in device_Dict.keys():
            device_Dict[device.manufacturer_id].append(device)
        else:
            device_Dict[device.manufacturer_id] = []
            device_Dict[device.manufacturer_id].append(device)
    return device_Dict
 
def get_device(deviceList):
    device_list = []
    for i in deviceList:
        device_list.append(db_DEVICE.objects.get(device_id=i,status=1))
    return device_list
 
 
def fetch_is_reference_grade(device_id):
   try:
       device_instance = db_DEVICE.objects.get(device_id=device_id)
       return device_instance.is_reference_grade
   except db_DEVICE.DoesNotExist:
       print(f"Device with ID '{device_id}' does not exist.")
       return False
