import requests
import json
from abc import ABC, abstractmethod
from datetime import datetime, timedelta,date
import pandas as pd
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from RawData.models import db_AirNet_Raw, db_AirNet_Raw_Response
import DjangoSetup
import pytz
from ast import literal_eval
from base64 import b64encode
 
spark = SparkSession.builder \
        .appName("Django Spark Integration") \
        .getOrCreate()
 
from core.Drivers.AirnetDriverAbs import AirnetDriverAbs
 
logger=logging.getLogger('workspace')
 
class aeron(AirnetDriverAbs):
    def __init__(self, manufacturer_obj):
        super().__init__(manufacturer_obj)
        self.fmt = "%Y-%m-%d %H:%M:%S"
        self.start_time = None
        self.end_time = None
        self._fetch_method = "GET"
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
        print("preprocess")
        try:
            
            self._auth_url = self.manufacturer_obj.auth_url
            self._authentication= {
            'username': self.manufacturer_obj.username,
            'password': self.manufacturer_obj.api_or_pass,
            'grant_type': self.manufacturer_obj.grant_type
            }
            client_name=self.manufacturer_obj.client_name
            client_secret=self.manufacturer_obj.client_secret
            self._headers = {
            'Host': self.manufacturer_obj.host,
            'Content-Type': self.manufacturer_obj.content_type,
            'Authorization': 'Basic ' + b64encode(f'{client_name}:{client_secret}'.encode()).decode(),
            'Accept': self.manufacturer_obj.accept
            }
            print("yes")
            response = requests.post(self._auth_url, data=self._authentication, headers=self._headers, verify=False)
            response_data = response.json()
            print(response_data)
            self.id_token = response_data['access_token']
            print(self.id_token)
 
            
            logger.info(f"Start time: {self.start_time}, End time: {self.end_time}")
        except Exception as e:
            logger.error(f"Error in preprocess: {e}")
 
    def process(self, deviceObj,dag_param):
        print("process")
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
            dt=date(2024,6,14)
            print(type(dt))
            #print(self.device_id)
            req = {
                    
                    'payload': {
                        'Host': self.manufacturer_obj.host,
                        'Authorization': 'Bearer ' + self.id_token,
                        'Accept': self.manufacturer_obj.accept
                    },
                    'headers': {'usn': '240091703829123', 'date': dt},
                    'url': self.manufacturer_obj.data_url
                }
            
            
            response = self.restPOST(req, deviceObj) if self._fetch_method == 'POST' else self.restGET(req, deviceObj)

            self._http_response = response
            self.creating_df(dev, req)
 
            # if self._df_list:
            #     self._df = pd.concat(self._df_list, axis=1)
            #     self._df['device_id'] = dev.device_id
            #     self._df_all_list.append(self._df)
            # else:
            #     logger.warning(f"No data fetched for device {dev.device_id}")
    def filter_params(self,raw_dict,parameter):
            
 
        channel = list(filter(lambda d: d['caption'].strip() == parameter, raw_dict))
        para_value = channel[0].get('value', None)
        return para_value


    def creating_df(self, deviceObj, request):
        try:
 
            print("inside df create")
            self.insert_raw_response(req_url=request['url'],dev_id=deviceObj.device_id,manufacturer_name=deviceObj.manufacturer_id.name, param=None)
            response_json= self._http_response.json()
            # print(response_json)
            data_batch=[]
            device_id=deviceObj.device_id
            for record in response_json:
                res_date = record.get('timestamp', None).get('date', None)
                res_time = record.get('timestamp', None).get('time', None)
                params_dict = record.get('params', None)
                o3_ppb = self.filter_params(params_dict, 'O3_ppb')
                so2_ppb = self.filter_params(params_dict, 'SO2_ppb')
                no2_ppb = self.filter_params(params_dict, 'NO2_ppb')
                co_ppm = self.filter_params(params_dict, 'CO_ppm')
                molecular_volume = self.filter_params(params_dict, 'MOLECULAR VOLUME')
                pressure = self.filter_params(params_dict, 'BAROMETRIC PRESSURE')
                o3_aux = self.filter_params(params_dict, 'O3_Aux')
                o3_we = self.filter_params(params_dict, 'O3_WEe')
                so2_aux = self.filter_params(params_dict, 'SO2_Aux')
                so2_we = self.filter_params(params_dict, 'SO2_WEe')
                no2_aux = self.filter_params(params_dict, 'NO2_Aux')
                no2_we = self.filter_params(params_dict, 'NO2_WEe')
                co_aux = self.filter_params(params_dict, 'CO_Aux')
                co_we = self.filter_params(params_dict, 'CO_WEe')
                tempering_alert = self.filter_params(params_dict, 'TEMPERING ALERT')
                humidity = self.filter_params(params_dict, 'RELATIVE HUMIDITY')
                temp = self.filter_params(params_dict, 'AMBIENT TEMPERATURE')
                pm10_alpha = self.filter_params(params_dict, 'PM10_ALPHA')
                pm25_alpha = self.filter_params(params_dict, 'PM2.5_ALPHA')
                pm1_alpha = self.filter_params(params_dict, 'PM1_ALPHA')
                pm10_tera = self.filter_params(params_dict, 'PM10_TERA')
                pm25_tera = self.filter_params(params_dict, 'PM2.5_TERA')
                pm1_tera = self.filter_params(params_dict, 'PM1_TERA')
                o3 = self.filter_params(params_dict, 'O3')
                so2 = self.filter_params(params_dict, 'SO2')
                no2 = self.filter_params(params_dict, 'NO2')
                co = self.filter_params(params_dict, 'CO')

                charging = record.get('health', None).get('charging', None)
                battery = record.get('health', None).get('battery', None)
                supply = record.get('health', None).get('supply', None)
                network = record.get('health', None).get('network', None)
                network_reg = record.get('health', None).get('network_reg', None)
                gpsfix = record.get('health', None).get('gpsfix', None)

                
 
                try:
                        latitude = record.get('location', None).get('latitude', None)
                except:
                        latitude = None
 
                try:
                        longitude = record.get('location', None).get('longitude', None)
                except:
                        longitude = None
 
                data_batch.append(
                        {'device_id': device_id, 'date': res_date, 'time': res_time, 'o3_ppb': o3_ppb, 'so2_ppb': so2_ppb,
                         'no2_ppb': no2_ppb, 'co_ppm': co_ppm, 'molecular_volume': molecular_volume,
                         'pressure': pressure, 'o3_aux': o3_aux, 'o3_we': o3_we, 'so2_aux': so2_aux,
                         'so2_we': so2_we, 'no2_aux': no2_aux, 'no2_we': no2_we, 'co_aux': co_aux,
                         'co_we': co_we, 'tempering_alert': tempering_alert, 'humidity': humidity, 'temp': temp,
                         'pm10_alpha': pm10_alpha, 'pm25_alpha': pm25_alpha, 'pm1_alpha': pm1_alpha,
                         'pm10_tera': pm10_tera, 'pm25_tera': pm25_tera, 'pm1_tera': pm1_tera,
                         'o3': o3, 'so2': so2, 'no2': no2, 'co': co, 'charging': charging, 'battery': battery,
                         'supply': supply, 'network': network, 'network_reg': network_reg, 'gpsfix': gpsfix,
                         'latitude': latitude, 'longitude': longitude
                         })
            df=pd.DataFrame(data_batch)
            print(df)
            if df.empty:
                self.store_missing_data_info(dev_obj=deviceObj,store_param=None)
 
                return
            df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'])
            df.drop(columns=['date', 'time'], inplace=True)
            print(df.columns)
            cal=df[['o3_ppb','so2_ppb','no2_ppb','co_ppm','o3','so2','no2','co','datetime','device_id']]
            df.drop(columns=['o3','so2','no2','co'], inplace=True)
            print(df.columns)
            null_counts = df.isnull().sum()
            print(df)
            print(null_counts)  
            
            
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
 
 
        except Exception as e:
            logger.error(f"Error in postprocess: {e}")
 
    def get_ColumnReplacement(self):
        try:
            _changeColumns = {'o3_ppb':'o3','so2_ppb':'so2','no2_ppb':'no2','co_ppm':'co','humidity':'relative_humidity','temp':'temperature','pm10_alpha':'pm10','pm25_alpha':'pm2_5','pm1_alpha':'pm1','pm10_tera':'pm10_opc','pm25_tera':'pm2_5_opc','pm1_tera':'pm1_opc','datetime':'time','pressure':'barometric_pressure'}
            diff_column = {
                'so2_nv': ['so2_we', 'so2_aux'],
                'no2_nv':['no2_we', 'no2_aux'],
                'o3_nv': ['o3_we', 'o3_aux'],
                'co_nv': ['co_we', 'co_aux'],
                
            }

            for column in _changeColumns:
                if column in self._df_all.columns:
                    self._df_all.rename(columns={column: _changeColumns[column]}, inplace=True)

    
            self._cal_df.rename(columns={'datetime':'time' }, inplace=True)

 
            for column in diff_column:
               col1, col2 = diff_column[column]
               if col1 in self._df_all.columns and col2 in self._df_all.columns:
                    self._df_all[column] = self._df_all[col1] - self._df_all[col2]
                    self._df_all.drop(columns=[col1, col2], inplace=True)       
        except Exception as e:
            logger.error(f"Error in get_ColumnReplacement: {e}")
 
    def handleDF(self):
            select_columns=['co','no2','so2','o3','co_nv','so2_nv','no2_nv','o3_nv','temperature','relative_humidity','pm1','pm2_5','pm10','pm1_opc','pm2_5_opc','pm10_opc','barometric_pressure','time','device_id']
            self._df_all=self._df_all[select_columns]
 
    def standardize_df(self):
        try:
            if not self._df_all.empty:
 
                self.get_ColumnReplacement()
                print(self._df_all)
                self.handleDF()
                print(self._df_all.columns)
            self.store_manufacturer_cal_data()
            
        except Exception as e:
            logger.error(f"Error in standardization_df: {e}")
 
    
    def filter_raw_gas(self, raw_dict, channel_num):
        channel = list(filter(lambda d: d['channel'] == channel_num, raw_dict))
        if len(channel) == 0:
            return 0, 0
        aux_avg = channel[0].get('aux_avg', None)
        act_avg = channel[0].get('act_avg', None)
        return aux_avg, act_avg
    

