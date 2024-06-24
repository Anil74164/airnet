from pyspark.sql import SparkSession # type: ignore
import json
import os
import pandas
import sys
import logging
import DjangoSetup
from core.models import db_DEVICE, db_MANUFACTURER,db_missing_data
from core.Drivers.AirVeda import AirVeda
from core.Drivers.DriverList import driverList as drivers
from core.Drivers.Respirer import respirer

import argparse
from datetime import datetime,timedelta
import pytz

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
    

    if args.start!=None:
        try:
            args.start = datetime.strptime(args.start, "%Y%m%d%H%M%S")
            print(type(args.start))
        except Exception as e:
            logger.error("Invalid date format:"+args.start)
    if args.end:
        try:
            args.end = datetime.strptime(args.end, "%Y%m%d%H%M%S")
        except Exception as e:
            logger.error("Invalid date format:"+args.end)
    
    return args


def update_missing_data_status(device_id, pollutant, start_time, end_time):
    try:
        logger.info("hnjkm,")
        logger.info(start_time)
        logger.info(end_time)
        
        ist_timezone = pytz.timezone('Asia/Kolkata')
        end_time=end_time.astimezone(ist_timezone)
        start_time=start_time.astimezone(ist_timezone)
        logger.info(start_time)
        logger.info(end_time)
        missing_data_entry = db_missing_data.objects.filter(
            device_id=device_id,
            parameter=pollutant,
            req_start_dt=start_time,
            req_end_dt=end_time,
            status=0  # Only update if status is 0 (not already received)
        ).first()
        logger.info(missing_data_entry)
          # Get the first matching entry
        if missing_data_entry:
            missing_data_entry.status = 1
            missing_data_entry.received_dt = datetime.now()
            missing_data_entry.save()
            logger.info(f"Marked status as 1 for missing data entry: {missing_data_entry}")
            return missing_data_entry
    except db_missing_data.DoesNotExist:
        pass
    except Exception as e:
        logger.error(f"Error updating missing data status: {e}")


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
    

    
def main_fetch(args=None):
    try:
        if args.start and args.end:

            # ist_timezone = pytz.timezone("Asia/Kolkata")
            # start= ist_timezone.localize(args.start)
            # start = start.astimezone(pytz.utc)
            # end= ist_timezone.localize(args.end)
            # end = end.astimezone(pytz.utc)
            start=args.start
            end=args.end
            print(type(start))
            print(start)
        else:
            end = datetime.now().replace(minute=(datetime.now().minute // 15) * 15, second=0, microsecond=0)
            start = end - timedelta(minutes=15)
            print(start," ",end)
        deviceList = []
        paramList = []
        print(args) 
        if args:
            if args.device:
                deviceList.append(args.device)
            if args.pollutant:
                paramList.append(args.pollutant)
        
        if len(deviceList)==0:
            data = get_all_devices()
        else:
            data = get_device(deviceList)


        device_Dict = fetchDeviceDict(data)
      
        print(paramList)
        print(drivers)
        print(device_Dict)
        for i in device_Dict.keys():
            try:
                print(i.name)
                if i.name in drivers:    
                    logger.info(f"Processing devices for manufacturer: {i.name}")
                
                    obj = drivers[i.name](manufacturer_obj=i)
                    print(obj)  
                    print("sucess")
                    da = obj.fetch(deviceObj=device_Dict[i],start=start,end=end,param=paramList)
                    print(da)
                    print(da.columns)
                    
                    
                    logger.info(f"Fetched data: {da}")

                    obj.standardize_df()
                    obj._df_all.to_csv('data.csv')
                    print(obj._missing_data_dict)
                    logger.info(f"Standardized DataFrame: {obj._df_all}")
                    logger.info(f"Missing data dictionary: {obj._missing_data_dict}")
                    print(obj._df_all)
                    if not obj._df_all.empty:
                        for device in device_Dict[i]:
                            logger.info(device_Dict[i])
                            print("azxscdfvgbasdfrgthyjjkASD")
                            if paramList:
                                for param in paramList:
                                    print("azxscdfvgbasdfrgthyjjkASD")      
                                    logger.info(paramList)
                                    updated_entry = update_missing_data_status(device, param, start, end)
                                    
                                    if updated_entry:
                                        logger.info(f"Updated status for missing data entry: {updated_entry}")
                            else:
                                    logger.info(paramList)
                                    updated_entry = update_missing_data_status(device_id=device,start_time=start,end_time=end,pollutant=None)
                                    
                                    if updated_entry:
                                        logger.info(f"Updated status for missing data entry: {updated_entry}")
                    print(obj._cal_df)
                    try:
                        obj.store_std_data()

                    except Exception as e:
                        logger.warning(f"Error processing devices for manufacturer {e}")
                    logger.info(f"Stored standardized data for manufacturer: {i.name}")
                    
            except Exception as e:
                logger.error(f"Error processing devices for manufacturer {i.name}: {e}")
                continue

    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        raise

if __name__ == "__main__":
    spark_init()
    # test_args = [
    #         'DataIngestion.py'
    #         '-d', '1210230117',
    #         '-d', '1210230136',
    #         '-p', 'pm10_base',
    #     ]
    
    # sys.argv = test_args

    args = get_options()
    print(args)
    main_fetch( args )

