
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
from ast import literal_eval

spark = SparkSession.builder \
        .appName("Django Spark Integration") \
        .getOrCreate()

from core.Drivers.AirnetDriverAbs import AirnetDriverAbs

logger=logging.getLogger('workspace')

class sensit_ramp(AirnetDriverAbs):
    def __init__(self, manufacturer_obj):
        super().__init__(manufacturer_obj)
        self.fmt = "%Y-%m-%d %H:%M:%S"
        self.start_time = None
        self.end_time = None
        self._fetch_method = "POST"
        self.id_token = ''
        self.device_id = None
        self._cal_http_response=None
        self._headers = {'Authorization': 'Bearer ' + self.id_token}
        self._auth_url = ''
        self._payload = {
            'deviceId': self.device_id,
            'startTime': self.start_time,
            'endTime': self.end_time
        }
        self._restprotocol['prefix'] = 'https'
        self._restprotocol['hostname'] = ''
        self._changeColumns = {'from': ['pm_2_5'], 'to': ['pm25']}
        self._df_all_list = []
        self._cal_df_list=[]
        


    def preprocess(self,start,end,deviceObj):
        #print("preprocess",start,end)
        try:
            self._authentication = {
                'email': self.manufacturer_obj.email,
                'password': self.manufacturer_obj.api_or_pass
            }
            #print(self._authentication)
            #print(self.manufacturer_obj.auth_url)
            response = requests.post(self.manufacturer_obj.auth_url, data=self._authentication)
            response_data = response.json()
            
            self.id_token = response_data['accessToken']
            # self.refresh_token = response_data.get('refreshToken')
            # self._expiry_duration = response_data['expiresIn']
            # end = datetime.now().replace(minute=(datetime.now().minute // 15) * 15, second=0, microsecond=0)
            # start = end - timedelta(minutes=15) 
            self.start_time=start
            self.end_time=end
            #print(self.start_time)
            #print(self.end_time)
            # self.start_time = self.start_time.astimezone(pytz.utc)
            # self.end_time = self.end_time.astimezone(pytz.utc)
            #print(self.start_time)
            #print(self.end_time)
            
            logger.info(f"Start time: {self.start_time}, End time: {self.end_time}")
        except Exception as e:
            logger.error(f"Error in preprocess: {e}")

    def process(self, deviceObj,dag_param):
        #print("process")
        #print(deviceObj)
        #print(dag_param)
        for dev in deviceObj:
            
            self.device_id = dev.device_id
            # if len(dag_param)!=0:
            #     paramList=dag_param
            # else:
            #     paramListStr = dev.parameters
            #     paramList = paramListStr.split(",")

            # paramList.append("o4")
            # paramList.append("so10")
            # print(paramList)
            # logger.info(f"Processing parameters: {paramList}")
            self._df_list = []
            self.time_added = False

            #print(self.device_id)
            req = {
                '_payload': {
                    'DeviceId': self.device_id,
                    'StartDate': self.start_time.strftime(self.fmt),
                    'EndDate': self.end_time.strftime(self.fmt)
                },
                '_headers': {'Authorization': 'Bearer ' + self.id_token},
                '_url': self.manufacturer_obj.data_url
            }
            
            response = self.restPOST(req, deviceObj) if self._fetch_method == 'POST' else self.restGET(req, deviceObj)

            self._http_response = response
            #print(response.json())
            self.creating_df(dev, req)

            # if self._df_list:
            #     self._df = pd.concat(self._df_list, axis=1)
            #     self._df['device_id'] = dev.device_id
            #     self._df_all_list.append(self._df)
            # else:
            #     logger.warning(f"No data fetched for device {dev.device_id}")
        


    def creating_df(self, deviceObj, request):
        try:

            print("inside df create")
            self.insert_raw_response(req_url=request['_url'],dev_id=deviceObj.device_id,manufacturer_name=deviceObj.manufacturer_id.name, param=None)
            data = self._http_response.json()['data']
            print(data)
            data_batch=[]
            for record in data:
                time = record.get('date', None)
                device_id = record.get('id', None)
                temp = record.get('T', None)
                humidity = record.get('RH', None)
                wind_speed = record.get('WS', None)
                wind_direction = record.get('WD', None)
                pt_pm1 = record.get('PM_1_1_0', None)
                pt_pm2_5 = record.get('PM_1_2_5', None)
                pt_pm10 = record.get('PM_1_10_0', None)
                opc_data = record.get('OPCData', {})
                opc_pm1 = opc_data.get('OPC_1_0', None)
                opc_pm2_5 = opc_data.get('OPC_2_5', None)
                opc_pm10 = opc_data.get('OPC_10_0', None)
                opc_temp = opc_data.get('OPC_T', None)
                opc_humidity = opc_data.get('OPC_RH', None)
                co = record.get('CO', None)
                co2 = record.get('CO2', None)
                o3 = record.get('O3', None)
                so2 = record.get('SO2', None)
                no2 = record.get('NO2', None)

                raw_val = record.get('raw')
                # print(raw_val)
               
                co_channel_num = 1
                so2_channel_num = 2
                no2_channel_num = 3
                o3_channel_num = 4
                co_aux_avg, co_act_avg = self.filter_raw_gas(raw_val, co_channel_num)
                so2_aux_avg, so2_act_avg = self.filter_raw_gas(raw_val, so2_channel_num)
                no2_aux_avg, no2_act_avg = self.filter_raw_gas(raw_val, no2_channel_num)
                o3_aux_avg, o3_act_avg = self.filter_raw_gas(raw_val, o3_channel_num)
                

                lat = record.get('lat', None)
                long = record.get('long', None)
                battery = record.get('BATT', None)
                charge = record.get('CHRG', None)

                data_batch.append(
                    {'time': time, 'device_id': device_id, 'temperature': temp, 'relative_humidity': humidity,
                        'wind_speed': wind_speed, 'wind_direction': wind_direction,
                        'pm1': pt_pm1, 'pm2_5': pt_pm2_5, 'pm10': pt_pm10,
                        'pm1_opc': opc_pm1, 'pm2_5_opc': opc_pm2_5, 'pm10_opc': opc_pm10,
                        'opc_temp': opc_temp, 'opc_humidity': opc_humidity,
                        'co': co, 'co2': co2, 'o3': o3, 'so2': so2,  'no2': no2,
                        'co_channel_num': co_channel_num, 
                        'no2_channel_num': no2_channel_num, 'o3_channel_num': o3_channel_num,
                        'so2_channel_num': so2_channel_num, 'co_aux_avg': co_aux_avg, 'co_act_avg': co_act_avg,
                        'no2_aux_avg': no2_aux_avg,
                        'no2_act_avg': no2_act_avg, 'o3_aux_avg': o3_aux_avg, 'o3_act_avg': o3_act_avg,
                        'so2_aux_avg': so2_aux_avg, 'so2_act_avg': so2_act_avg,
                        'lat': lat, 'long': long, 'battery': battery, 'charge': charge
                        })
            df=pd.DataFrame(data_batch)
            if df.empty:
                self.store_missing_data_info(dev_obj=deviceObj,store_param=None)

                return

            cal=df[['co','o3','so2','no2','time','device_id']]
            
            print(df.columns)
            null_counts = df.isnull().sum()
            print(df)
            print(null_counts)  
            
            print(null_counts['time'])
            print(len(df))
            print(cal.columns)
            for column in df.columns:
                if null_counts[column]==len(df):
                    self.store_missing_data_info(dev_obj=deviceObj,store_param=column)

            print(cal)
            self._cal_df_list.append(cal)
            self._df_all_list.append(df)
                    
        except Exception as e:
            logger.error(f"Error in creating_df: {e}")

    def postprocess(self, deviceObj):
        try:
            if self._df_all_list:
                self._df_all = pd.concat(self._df_all_list)
            else:
                self._df_all = pd.DataFrame()

            if self._cal_df_list:
                self._cal_df = pd.concat(self._cal_df_list)
            else:
                self._cal_df = pd.DataFrame()

            self.store_manufacturer_cal_data()

        except Exception as e:
            logger.error(f"Error in postprocess: {e}")

    def get_ColumnReplacement(self):
        try:
            _changeColumns = {}
            diff_column = {
                'so2_nv': ['so2_act_avg', 'so2_aux_avg'],
                'no2_nv':['no2_act_avg', 'no2_aux_avg'],
                'o3_nv': ['o3_act_avg', 'o3_aux_avg'],
                'co_nv': ['co_act_avg', 'co_aux_avg'],
                
            }
    
            for column in _changeColumns:
                if column in self._df_all.columns:
                    self._df_all.rename(columns={column: _changeColumns[column]}, inplace=True)

            for column in diff_column:
               col1, col2 = diff_column[column]
               if col1 in self._df_all.columns and col2 in self._df_all.columns:
                    self._df_all[column] = self._df_all[col1] - self._df_all[col2]
                    self._df_all.drop(columns=[col1, col2], inplace=True)       
        except Exception as e:
            logger.error(f"Error in get_ColumnReplacement: {e}")

    def handleDF(self):
            select_columns=['co','no2','so2','o3','co2','co_nv','so2_nv','no2_nv','o3_nv','temperature','relative_humidity','pm1','pm2_5','pm10','pm1_opc','pm2_5_opc','pm10_opc','time','device_id']
            self._df_all=self._df_all[select_columns]
            self._df_all['time'] = pd.to_datetime(self._df_all['time'])
            self._df_all['time'] = self._df_all['time'].dt.tz_convert('Asia/Kolkata')
            

    def standardize_df(self):
        try:
            if not self._df_all.empty:

                self.get_ColumnReplacement()
                self.handleDF()
                print(self._df_all)
                print(self._df_all.columns)
            
        except Exception as e:
            logger.error(f"Error in standardize_df: {e}")

    
    def filter_raw_gas(self, raw_dict, channel_num):
        channel = list(filter(lambda d: d['channel'] == channel_num, raw_dict))
        if len(channel) == 0:
            return 0, 0
        aux_avg = channel[0].get('aux_avg', None)
        act_avg = channel[0].get('act_avg', None)
        return aux_avg, act_avg