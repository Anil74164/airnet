
import requests
import json
from abc import ABC, abstractmethod
from datetime import datetime, timedelta 
import pandas as pd
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from RawData.models import db_AirNet_Raw, db_AirNet_Raw_Response
import DjangoSetup
import pytz

spark = SparkSession.builder \
        .appName("Django Spark Integration") \
        .getOrCreate()

from core.Drivers.AirnetDriverAbs import AirnetDriverAbs

logger=logging.getLogger('workspace')

class AirVeda(AirnetDriverAbs):
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
        


    def preprocess(self,start,end,deviceObj):
        print("preprocess")
        try:
            self._authentication = {
                'email': self.manufacturer_obj.email,
                'password': self.manufacturer_obj.api_or_pass
            }
            print(self._authentication)
            print(self.manufacturer_obj.auth_url)
            response = requests.post(self.manufacturer_obj.auth_url, data=self._authentication)
            response_data = response.json()
            
            self.id_token = response_data['idToken']
            self.refresh_token = response_data.get('refreshToken')
            self._expiry_duration = response_data['expiresIn']
            # end = datetime.now().replace(minute=(datetime.now().minute // 15) * 15, second=0, microsecond=0)
            # start = end - timedelta(minutes=15) 
            self.start_time=start
            self.end_time=end
            print(self.start_time)
            print(self.end_time)
            # self.start_time = self.start_time.astimezone(pytz.utc)
            # self.end_time = self.end_time.astimezone(pytz.utc)
            print(self.start_time)
            print(self.end_time)
            
            logger.info(f"Start time: {self.start_time}, End time: {self.end_time}")
        except Exception as e:
            logger.error(f"Error in preprocess: {e}")

    def process(self, deviceObj,dag_param):
        print("process")
        print(deviceObj)
        print(dag_param)
        for dev in deviceObj:
            self.fetch_cal(deviceObj=dev)
            self.device_id = dev.device_id
            if len(dag_param)!=0:
                paramList=dag_param
            else:
                paramListStr = dev.parameters
                paramList = paramListStr.split(",")

            print(paramList)
            logger.info(f"Processing parameters: {paramList}")
            self._df_list = []
            self.time_added = False

            for param in paramList:
                req = {
                    'param': param,
                    '_payload': {
                        'refreshToken': self.refresh_token,
                        'deviceId': self.device_id,
                        'pollutant': param,
                        'startTime': self.start_time.strftime(self.fmt),
                        'endTime': self.end_time.strftime(self.fmt)
                    },
                    '_headers': {'Authorization': 'Bearer ' + self.id_token},
                    '_url': self.manufacturer_obj.data_url,
                    'json_payload':None
                }
                
                response = self.restPOST(req, deviceObj) if self._fetch_method == 'POST' else self.restGET(req, deviceObj)

                self._http_response = response
                self.creating_df(dev, req)

            if self._df_list:
                self._df = pd.concat(self._df_list, axis=1)
                self._df['device_id'] = dev.device_id
                self._df_all_list.append(self._df)
               
               
            else:
                logger.warning(f"No data fetched for device {dev.device_id}")
        if self._cal_df_list:
            self._cal_df = pd.concat(self._cal_df_list)
            if 'aqi' in self._cal_df.columns:    
                self._cal_df=self._cal_df.drop(['aqi'],axis=1)
            if 'battery' in self._cal_df.columns:
                self._cal_df=self._cal_df.drop(['battery'],axis=1)
        else:
            self._cal_df = pd.DataFrame()
        print("ccccccccccccccccccccccc")
        print(self._cal_df)
        self._cal_df
        self.store_manufacturer_cal_data()


    def creating_df(self, deviceObj, request):
        try:

            print("inside df create")
            self.insert_raw_response(req_url=request['_url'],dev_id=deviceObj.device_id, manufacturer_name=deviceObj.manufacturer_id.name, param=request['param'])
            df = pd.DataFrame(self._http_response.json())
            df = df['readings'].apply(pd.Series)
            print(df)

            if df.empty:
                self.store_missing_data_info(dev_obj=deviceObj,store_param=request['param'])
                return

            df['time'] = pd.to_datetime(df['time'], format="%Y-%m-%d %H:%M:%S")
            if not self.time_added:
                self._df_list.append(df[['time']])
                self.time_added = True

            df.columns = ['{}{}'.format(c, '' if c == 'time' else '_' + request['param']) for c in df.columns]
            df.set_index('time', inplace=True)
            df.reset_index(drop=True, inplace=True)
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
            _changeColumns = {'value_pm25_base': 'pm2_5_r', 'value_pm10_base': 'pm10_r','humidity' : 'relative_humidity'}
            diff_column = {
                'so2_nv': ['WorkingElectrodeVoltage_so2Voltages', 'AuxilliaryElectrodeVoltage_so2Voltages'],
                'no2_nv': ['WorkingElectrodeVoltage_no2Voltages', 'AuxilliaryElectrodeVoltage_no2Voltages'],
                'o3_nv': ['WorkingElectrodeVoltage_ozoneVoltages', 'AuxilliaryElectrodeVoltage_ozoneVoltages'],
                'co_nv': ['WorkingElectrodeVoltage_coVoltages', 'AuxilliaryElectrodeVoltage_coVoltages']
            }
            for column in _changeColumns:
                print(self._df_all.columns)
                print("zzzzzzzzz")
                if column in self._df_all.columns:
                    self._df_all.rename(columns={column: _changeColumns[column]}, inplace=True)

            for column in diff_column:
                if diff_column[column][0] in self._df_all.columns:
                    self._df_all[column] = self._df_all[diff_column[column][0]] - self._df_all[diff_column[column][1]]
                    self._df_all.drop(columns=[diff_column[column][0], diff_column[column][1]], inplace=True)
        except Exception as e:
            logger.error(f"Error in get_ColumnReplacement: {e}")

    def handleDF(self):
        try:
            # dict1 = {'co2': 'co2_cov', 'no2': 'no2_cov', 'so2': 'so2_cov', 'no': 'no_cov', 'o3': 'o3_cov'}
            # for column in self._df_all.columns:
            #     if column in dict1:
            #         self.dict1[column](self._df_all, column)
            self._df_all['time'] = pd.to_datetime(self._df_all['time'], errors='coerce')
            self._df_all['time'] = self._df_all['time'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
            
            if not self._df_all.empty and not self._cal_df.empty    : 
                join_df=self._df_all
                
                print(join_df)
                self._df_all = pd.merge(join_df, self._cal_df, on=['device_id','time'], how='inner')
                print(self._df_all.columns)
                self._df_all=self._df_all.drop(['pm25','ozone','pm10','co','so2','no2'], axis=1)
                self._df_all.rename(columns={'humidity': 'relative_humidity'}, inplace=True)
            print("aaaaaaaaaaaaaaaaaaa")
            print(self._df_all.columns)
            
          
        except Exception as e:
            logger.error(f"Error in handleDF: {e}")

    def standardize_df(self):
        try:
            if not self._df_all.empty:
                self.get_ColumnReplacement()
                self.handleDF()

            
        except Exception as e:
            logger.error(f"Error in standardization_df: {e}")

    def fetch_cal(self,deviceObj):
        cal_req = {
            '_payload': {
                'deviceId': deviceObj.device_id,
            },
            '_headers': {'Authorization': 'Bearer ' + self.id_token},
            '_url': self.manufacturer_obj.cal_url
        }
        
        
        if self._fetch_method == 'POST':
            response = self.restPOST(cal_req, deviceObj)
        else:
            response = self.restGET(cal_req, deviceObj)
            
        self._cal_http_response = response.json()

        cal_df=pd.DataFrame(self._cal_http_response)
        cal_df=cal_df['data'].apply(pd.Series)
        
        cal_df['device_id'] = deviceObj.device_id
        

        cal_df['time'] = pd.to_datetime(cal_df['time'])
        
        # Set 'datetime' column as the index
        cal_df.set_index('time', inplace=True)

        # Define the start and end times
        

        # Filter the DataFrame between the start and end times
        cal_df= cal_df.loc[self.start_time:self.end_time]
        cal_df.reset_index(inplace=True)
        cal_df['time'] = pd.to_datetime(cal_df['time'], errors='coerce')
        cal_df['time'] = cal_df['time'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
        print(cal_df)
        print(cal_df.columns)
        self._cal_df_list.append(cal_df)


        