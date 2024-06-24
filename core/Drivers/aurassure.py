import requests
import json
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import pandas as pd
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import DjangoSetup
from RawData.models import db_AirNet_Raw, db_AirNet_Raw_Response
import time
import pytz
 
spark = SparkSession.builder \
        .appName("Django Spark Integration") \
        .getOrCreate()
 
from core.Drivers.AirnetDriverAbs import AirnetDriverAbs
 
logger=logging.getLogger('workspace')
 
class aurassure(AirnetDriverAbs):
    def __init__(self, manufacturer_obj):
        super().__init__(manufacturer_obj)
        self.fmt = "%Y-%m-%d %H:%M:%S"
        self.start_time = None
        self.end_time = None
        self._fetch_method = "POST"
        self.id_token = ''
        self.refresh_token =None
        self.device_id = None
        self.param = None
        self._cal_http_response=None
        self._headers = {'Authorization': 'Bearer ' + self.id_token}
        self._auth_url = ''
        self._payload = {
            'refreshToken': self.refresh_token,
            'deviceId': self.device_id,
            'pollutant': self.param,
            'startTime': self.start_time,
            'endTime': self.end_time
        }
        self._restprotocol['prefix'] = 'https'
        self._restprotocol['hostname'] = ''
        self._changeColumns = {'from': ['pm_2_5'], 'to': ['pm25']}
        self._df_all_list = []
        self._cal_df_list=[]
        
 
 
    def preprocess(self, start ,end,deviceObj):
        print("preprocess")
        try:
            self.end_time = end
            self.start_time = start
            self.end_time_unix= time.mktime(self.end_time.timetuple())
            self.start_time_unix = time.mktime(self.start_time.timetuple())
            print(self.end_time)
            print(self.start_time)
            logger.info(f"Start time: {self.start_time}, End time: {self.end_time}")
        except Exception as e:
            logger.error(f"Error in preprocess: {e}")
 
    def process(self, deviceObj,dag_param):
       
        for dev in deviceObj:
            self.fetch_cal(deviceObj=dev)
            self.device_id = dev.device_id
            if len(dag_param)!=0:
                paramList=dag_param
            else:
                paramListStr = dev.parameters
                paramList = paramListStr.split(",")
    
            
            logger.info(f"Processing parameters: {paramList}")
            self._df_list = []
            self.time_added = False
 
            for param in paramList:
            
                req = {
                    'param': param,
                    '_payload': {
                        "data_type": "raw",
                        "aggregation_period": 0,
                        "parameters": [param],
                        "parameter_attributes": [],
                        "things": [int(self.device_id)],
                        "from_time": self.start_time_unix,
                        "upto_time": self.end_time_unix,
                    },
                    '_headers': {"Access-Id": self.manufacturer_obj.access_id,
                                "Access-Key": self.manufacturer_obj.access_key,
                                "Content-Type": self.manufacturer_obj.content_type},
                    '_url': self.manufacturer_obj.data_url
                }
                req['_payload']=json.dumps(req['_payload'])
                # print(req)    
                response = self.restPOST(req, deviceObj) if self._fetch_method == 'POST' else self.restGET(req, deviceObj)
                self._http_response = response
                
                self.creating_df(dev, req)
            
            if self._df_list:
                self._df = pd.concat(self._df_list, axis=1)
                self._df['device_id'] = dev.device_id
                self._df_all_list.append(self._df)
            else:
                logger.warning(f"No data fetched for device {dev.device_id}")
       
 
        
 
    def creating_df(self, deviceObj, request):
        try:
            self.insert_raw_response(req_url=request['_url'],dev_id=deviceObj.device_id, manufacturer_name=deviceObj.manufacturer_id.name, param=request['param'])
            print("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

            print(self._http_response.json())
            df = pd.DataFrame(self._http_response.json()['data'])
            print(df)
            # df = df['data'].apply(pd.Series)
            if df.empty:
                self.store_missing_data_info(dev_obj=deviceObj,store_param=request['param'])
                return
            df[request['param']] = df['parameter_values'].apply(lambda x: float(x[request['param']]))
            if not self.time_added:
                self._df_list.append(df[['time']])
                self.time_added = True

            df.set_index('time', inplace=True)
            df.reset_index(drop=True, inplace=True)
            df = df.drop(columns=['parameter_values','thing_id'])
            print(df)
            self._df_list.append(df)
        except Exception as e:
            logger.error(f"Error in creating_df: {e}")
 
    def postprocess(self, deviceObj):
        try:
            if self._df_all_list:
                self._df_all = pd.concat(self._df_all_list)
            else:
                self._df_all = pd.DataFrame()
 
        except Exception as e:
            logger.error(f"Error in postprocess: {e}")
 
    def get_ColumnReplacement(self):
        try:
            _changeColumns = {'humid':'relative_humidity','pm2.5':'pm2_5','temp':'temperature'}
           
    
            for column in _changeColumns:
                if column in self._df_all.columns:
                    self._df_all.rename(columns={column: _changeColumns[column]}, inplace=True)

                  
        except Exception as e:
            logger.error(f"Error in get_ColumnReplacement: {e}")
 
    def handleDF(self):
        self._df_all['time'] = self._df_all['time'].apply(self.unix_to_ist)
 
    def standardize_df(self):
        try:
            self.get_ColumnReplacement()
            self.handleDF()
        except Exception as e:
            logger.error(f"Error in standardization_df: {e}")
 
    def fetch_cal(self,deviceObj):
        pass
       
        
    def unix_to_ist(self,unix_time):
        # Define the IST timezone
        ist = pytz.timezone('Asia/Kolkata')
        # Convert the Unix timestamp to a datetime object in UTC
        utc_dt = datetime.utcfromtimestamp(unix_time).replace(tzinfo=pytz.utc)
        # Convert the datetime object to IST
        ist_dt = utc_dt.astimezone(ist)
        return ist_dt