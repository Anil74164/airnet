
 
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
 
spark = SparkSession.builder \
        .appName("Django Spark Integration") \
        .getOrCreate()
from core.Drivers.AirnetDriverAbs import AirnetDriverAbs
 
logger=logging.getLogger('workspace')
 
class respirer(AirnetDriverAbs):
    def __init__(self, manufacturer_obj):
        super().__init__(manufacturer_obj)
        self.fmt = "%Y-%m-%d %H:%M:%S"
        self.start_time = None
        self.end_time = None
        self._fetch_method = "GET"
        self.device_id = None
        self.param = None
        self._cal_http_response=None
        self._changeColumns = {'from': ['pm_2_5'], 'to': ['pm25']}
        self._df_all_list = []
        self._cal_df_list=[]
    
 
 
    def preprocess(self,start,end,deviceObj):
        
        try:
            self._authentication = {
                'base_url': self.manufacturer_obj.data_url,
                'password': self.manufacturer_obj.api_or_pass
            }
            self.end_time =end
            self.start_time = start
    
           
        except Exception as e:
            logger.error(f"Error in preprocess: {e}")
 
    def process(self, deviceObj,dag_param):
        
        
        for dev in deviceObj:
            self.device_id = dev.device_id
            paramListStr=None
            # paramList=None
            if len(dag_param)!=0:
                paramListStr=','.join(dag_param)
            else:
                paramListStr = dev.parameters
                # paramList = paramListStr.split(",")
           
            logger.info(f"Processing parameters: {paramListStr}")
            self._df_list = []
            self.time_added = False
            # for param in paramList:
            #     print(self._authentication['base_url'])
            #     print(self.device_id)
            #     print(param)
        
            
            req={
                'url' : (self._authentication['base_url'] + self.device_id + '/params/' + paramListStr + '/startdate/' + self.start_time.strftime(self.fmt) +'/enddate/' + self.end_time.strftime(self.fmt) + '/ts/mm/avg/1/api/' + self._authentication['password'] + "?gaps=1&gap_value=NaN&json=1"),
                'headers':None,
                'payload':None
            }
            response = self.restPOST(req, deviceObj) if self._fetch_method == 'POST' else self.restGET(req,deviceObj)
            
            self._http_response = response
    

            self.creating_df(dev,req,paramListStr)
            # if self._df_list:
            #     print("yessssssssssssssssss")
            #     self._df = pd.concat(self._df_list, axis=1)
            #     self._df_all_list.append(self._df)
            #     print(self._df_all_list)
               
            # else:
            #     logger.warning(f"No data fetched for device {dev.device_id}")
            
 
    def creating_df(self, deviceObj,req,param):
        try:
   
           
            
        
            self.insert_raw_response(req_url=req['url'],dev_id=deviceObj.device_id, manufacturer_name=deviceObj.manufacturer_id.name,param=param)
            df = pd.DataFrame(self._http_response.json())
            
            
 
            if df.empty:    
                self.add_missing_data(device=deviceObj, param=req['param'], error_code=self._http_response.status_code)
                return
 
            # df['time'] = pd.to_datetime(df['time'], format="%Y-%m-%d %H:%M:%S")
            # if not self.time_added:
            #     self._df_list.append(df[['time']])
            #     self.time_added = True
 
            
            # df.set_index('time', inplace=True)
            df.reset_index(drop=True, inplace=True)
           
            self._df_all_list.append(df)
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
            _changeColumns = {'pm2.5cnc': 'pm2_5', 'pm10cnc': 'pm10','temp':'temperature','humidity':'relative_humidity','opc_r2_pm1':'pm1_opc','opc_r2_pm25':'pm2_5_opc','opc_r2_pm10':'pm10_opc','deviceid':'device_id','dt_time':'time'}
            diff_column = {
                'no2_nv': ['no2op1', 'no2op2'],
                'so2_nv': ['so2op1', 'so2op2'],
                'o3_nv': ['o3op1', 'o3op2'],
                'co_nv': ['coop1', 'coop2']
            }
            for column in _changeColumns:
                if column in self._df_all.columns:
                    self._df_all.rename(columns={column: _changeColumns[column]}, inplace=True)

            for column in diff_column:
               col1, col2 = diff_column[column]
          
               if col1 in self._df_all.columns and col2 in self._df_all.columns:
                   
                    self._df_all[col1] = pd.to_numeric(self._df_all[col1], errors='coerce')
                    self._df_all[col2] = pd.to_numeric(self._df_all[col2], errors='coerce')
                    self._df_all[column] = self._df_all[col1] - self._df_all[col2]
                    
                    self._df_all.drop(columns=[col1, col2], inplace=True) 
            self._df_all['time'] = pd.to_datetime(self._df_all['time'])
            
        except Exception as e:
            logger.error(f"Error in get_ColumnReplacement: {e}")
 
    
    def standardize_df(self):
        try:
            if not self._df_all.empty:
               
                self.get_ColumnReplacement()
                self.handleDF()
              
            
            
        except Exception as e:
            logger.error(f"Error in standardization_df: {e}")

    

    
    def handleDF(self):
        self._df_all['time']=self._df_all['time'].dt.tz_localize("GMT")
        self._df_all['time']=self._df_all['time'].dt.tz_convert("Asia/Kolkata")
        
 
    
        
        
        