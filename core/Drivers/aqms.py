
 
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
 
class aqms(AirnetDriverAbs):
    def __init__(self,manufacturer_obj):
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
        # self._restprotocol['prefix'] = 'https'
        # self._restprotocol['hostname'] = ''
        self._changeColumns = {'from': ['pm_2_5'], 'to': ['pm25']}
        self._df_all_list = []
        self._cal_df_list=[]
        self.met_df=pd.DataFrame()
        self.par_df=pd.DataFrame()
        
 
 
    def preprocess(self,start,end,deviceObj):
        self.start_time= start
        self.end_time= end
        ist_timezone = pytz.timezone('Asia/Kolkata')
        self.end_time=self.end_time.astimezone(ist_timezone)
        self.start_time=self.start_time.astimezone(ist_timezone)
 
    def process(self, deviceObj,dag_param):
 
        
        for dev in deviceObj:
            json_data = {
            "cmd": 777,
            "industry_id": "795",
            "station_id": dev.device_id,
            "from_date": str(self.start_time),
            "to_date": str(self.end_time)
            }
            js=json.dumps(json_data)
          
            # print(response.json())
            # json_data = json.dumps(response.json())
            # data = json.loads(json_data)
            # param = data['parameters'].split(',')
            # report_data = data['report_data']
 
            # #columns = response.parameters.split(',')
            # # data = response.report_data
            # df = pd.DataFrame(columns=param, data=report_data)
            # #df = pd.DataFrame(columns=response['parameters'].split(","), data=response.get('report_data', []))
            # print(df)
            # print(df.columns)
 
           
            # logger.info(f"Processing parameters: {paramList}")
            self._df_list = []
            self.time_added = False
 
            req = {
                    '_headers': {'Content-Type': 'application/json'},
                    '_url': "https://datalogger.vasthienviro.com/",
                    '_payload': js
            }
                
            response = self.restPOST(req, deviceObj) if self._fetch_method == 'POST' else self.restGET(req, deviceObj)
 
            self._http_response = response.json()
      
            self.creating_df(dev, req)
 
            
               
               
            
     
 
       
 
 
    def creating_df(self, deviceObj, request):
        try:
 
           
            #self.insert_raw_response(req_url=request['_url'],dev_id=deviceObj.device_id, manufacturer_name=deviceObj.manufacturer_id.name, param=None)
            json_data = json.dumps(self._http_response)
            data = json.loads(json_data)
            param = data['parameters'].split(',')
            report_data = data['report_data']
 
     
            df = pd.DataFrame(columns=param, data=report_data)
          
      
            if self.met_df.empty:
               
                self.met_df=df
                self.met_df['device_id']=deviceObj.device_id
                 
                
            else:
                
                self.par_df=df
                
            
            
        

            if df.empty:
                self.store_missing_data_info(dev_obj=deviceObj,store_param=None)
                return
 
          
            
            self._df_all_list.append(df)
           
        
           
        except Exception as e:
            logger.error(f"Error in creating_df: {e}")
 
    def postprocess(self, deviceObj):
        try:
               
                self._df_all = pd.merge(self.met_df, self.par_df, on=['DateTime'], how='inner')
                
            
        
 
        except Exception as e:
            logger.error(f"Error in postprocess: {e}")
 
    def get_ColumnReplacement(self):
        try:
            _changeColumns = {'Ambient_Temp': 'temperature', 'Humidity': 'relative_humidity','DateTime':'time','WIND_SPEED':'wind_speed','RAIN_FALL':'rain_fall','WIND_DIRECTION':'wind_direction','Barometric_Pressure':'barometric_pressure','PM10':'pm10','CO':'co','SO2':'so2','NO':'no','NO2':'no2','O3':'o3','PM2.5':'pm2_5','NOx':'nox'}
            # diff_column = {
            #     'so2_nv': ['WorkingElectrodeVoltage_so2Voltages', 'AuxilliaryElectrodeVoltage_so2Voltages'],
            #     'no2_nv': ['WorkingElectrodeVoltage_no2Voltages', 'AuxilliaryElectrodeVoltage_no2Voltages'],
            #     'o3_nv': ['WorkingElectrodeVoltage_ozoneVoltages', 'AuxilliaryElectrodeVoltage_ozoneVoltages'],
            #     'co_nv': ['WorkingElectrodeVoltage_coVoltages', 'AuxilliaryElectrodeVoltage_coVoltages']
            # }
            for column in _changeColumns:
                if column in self._df_all.columns:
                    print('sndxfmc.g')
                    self._df_all.rename(columns={column: _changeColumns[column]}, inplace=True)
        
 
            # for column in diff_column:
            #     if diff_column[column][0] in self._df_all.columns:
            #         self._df_all[column] = self._df_all[diff_column[column][0]] - self._df_all[diff_column[column][1]]
            #         self._df_all.drop(columns=[diff_column[column][0], diff_column[column][1]], inplace=True)
        except Exception as e:
            logger.error(f"Error in get_ColumnReplacement: {e}")
 
    def handleDF(self):
        self._df_all['time'] = pd.to_datetime(self._df_all['time']).dt.tz_localize('Asia/Kolkata')
    def standardize_df(self):
        try:
            if not self._df_all.empty:
                self.get_ColumnReplacement()
                self.handleDF()
                
            
        except Exception as e:
            logger.error(f"Error in standardization_df: {e}")
 
    def fetch_cal(self,deviceObj):
        pass
 
        
    # def fetch(self,start,end,param=None, deviceObj=None):
    #     try:
    #         print("fetch",start,end)
    #         self.preprocess(deviceObj=deviceObj,start=start,end=end)
    #         self.process(deviceObj=deviceObj,dag_param=param)
    #         self.postprocess(deviceObj)
    #         # self.store_missing_data_info()
    #         print(self._df_all)
    #         return self._df_all
    #     except Exception as e:
    #         logger.error(f"Error in fetch: {e}")
    #         raise      
        
 