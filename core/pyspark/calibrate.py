from pyspark.sql import SparkSession
import pandas as pd
import DjangoSetup
import pickle
import logging
from core.models import db_std_data, db_DEVICE, db_AirNet_Aggregated, db_calibration_models,db_AQData,db_aq_aggregated
from core.pyspark.config import *
from core.pyspark.common import * 
import pytz



logger=logging.getLogger('workspace')

def spark_init():
    spark = SparkSession.builder \
        .appName("Django Spark Integration") \
        .getOrCreate()
    return spark

def fetch_std_data(start_date, end_date):
    try:
        # Ensure date format is correct
        print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        
        start_time=start_date
        end_time=end_date
        logger.info(f"Filtering data from {start_date} to {end_date}")

    
        # Fetch filtered data
        filtered_std_data = db_std_data.objects.filter(time__range=(start_time, end_time)).values()
        print(filtered_std_data)
        # Log the SQL query being executed
        logger.info(f"SQL Query: {filtered_std_data}")

        # Convert to DataFrame
        std_data_df = pd.DataFrame(filtered_std_data)

        logger.info(f"Fetched {len(std_data_df)} records from db_std_data")
        return std_data_df

    except Exception as e:
        logger.error(f"Error in fetching data: {e}")
        return pd.DataFrame()



def get_std_data(start_date,end_date):
    try:
        print(start_date,end_date)
        print(type(start_date))
        
        filtered_std_data=db_std_data.objects.filter(time__range=(start_date, end_date)).values()
        print(filtered_std_data)
        std_data_df=pd.DataFrame(filtered_std_data)

        return std_data_df

    except Exception as e:
        logger.error(f"Error in fetching data: {e}")
        return pd.DataFrame()
    
def get_agg_data(start_date,end_date):
    try:
        
        
        filtered_agg_data=db_AirNet_Aggregated.objects.filter(time__range=(start_date, end_date)).values()
        print(filtered_agg_data)
        agg_data_df=pd.DataFrame(filtered_agg_data)

        return agg_data_df

    except Exception as e:
        logger.error(f"Error in fetching data: {e}")
        return pd.DataFrame()


def cstep_calibrate(df,method=None,pollutant=None,device_id=None):
   df_new = df
   return df_new

def aqdata(db_std_data):
   # Initialize an empty DataFrame to hold the results
   aqdata_result = pd.DataFrame()
   #print(db_std_data)
   # Get the list of unique device_ids in std_data
   unique_device_ids = db_std_data['device_id_id'].unique()
   #print(unique_device_ids)

   #TODO: try except 
   config=AirnetConfig()
   method = config['Calibration']['method']
   #print("method " + method)

   # Process each device_id
   for device_id in unique_device_ids:
       is_reference_grade = fetch_is_reference_grade(device_id)

       # Filter std_data for the current device_id
       temp_df = db_std_data[db_std_data['device_id_id'] == device_id]
       print(f"Processing data of {device_id}.reference_grade={is_reference_grade}")
       #print(temp_df)

       #TODO: check for an empty temp_df

       if is_reference_grade:  
           # If is_reference_grade is True, concatenate the temp_df directly
           aqdata_result = pd.concat([aqdata_result, temp_df])
           
       else:
           print(f"Calibrating device {device_id} ")
           # If is_reference_grade is False, perform calibration and then concatenate
           calibrated_df = cstep_calibrate(temp_df,method)
           
           aqdata_result = pd.concat([aqdata_result, calibrated_df])
       

   return aqdata_result


def aq_aggregated_data(db_AirNet_Aggregated):
   # Initialize an empty DataFrame to hold the results
   aggregated_data_result = pd.DataFrame()

   # Get the list of unique device_ids in aggregated_df
   unique_device_ids = db_AirNet_Aggregated['device_id_id'].unique()

    #TODO: check for an empty temp_df
   config=AirnetConfig()
   method = config['Calibration']['method']

   # Process each device_id
   for device_id in unique_device_ids:
       is_reference_grade = fetch_is_reference_grade(device_id)

       # Filter aggregated_df for the current device_id
       temp_df = db_AirNet_Aggregated[db_AirNet_Aggregated['device_id_id'] == device_id]
       print(f"Processing data of {device_id}.reference_grade={is_reference_grade}")

       #TODO: check for an empty temp_df
       if is_reference_grade:
           # If is_reference_grade is True, concatenate the temp_df directly
           aggregated_data_result = pd.concat([aggregated_data_result, temp_df])
       else:
           print(f"Calibrating device {device_id} ")
           # If is_reference_grade is False, perform calibration and then concatenate
           calibrated_df = cstep_calibrate(temp_df,method)
           aggregated_data_result = pd.concat([aggregated_data_result, calibrated_df])

   return aggregated_data_result



def process_and_store_aqdata(start_date,end_date):
   # Fetch std_data from the db_std_data model
   std_data=get_std_data(start_date, end_date)
#    print(std_data)
#    print('*' * 50)

   # Process the std_data using aqdata
   aqdata_result = aqdata(std_data)
   print(aqdata_result)
   #aqdataResult df store to table
   try:
       for _, row in aqdata_result.iterrows():
           db_AQData.objects.create(
               **row
           ).save()

       print("Calibrated data saved")
       
   except Exception as e:
       print(f"An error occurred while storing aggregated data: {str(e)}")

   return aqdata_result

def process_and_store_aggregated_data(start_date,end_date):
   # Fetch aggregated_df from the AirNet_Aggregated model
   aggregated_data = get_agg_data(start_date,end_date)


   # Process the aggregated_df using aq_aggregated_data
   aggregated_data_result = aq_aggregated_data(aggregated_data)

   try:
       for _, row in aggregated_data_result.iterrows():
           db_aq_aggregated.objects.create(
               **row
           ).save()
       print("Calibrated aggregated data saved")
   except Exception as e:
       print(f"An error occurred while storing aggregated data: {str(e)}")


   return aggregated_data_result


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
    #std_data=get_std_data(start_date=args.start,end_date=args.end)
    #aqdata(std_data)
    process_and_store_aqdata( args.start, args.end )
    process_and_store_aggregated_data( args.start, args.end )