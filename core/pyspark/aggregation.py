


import DjangoSetup
from core.models import db_std_data, db_AirNet_Aggregated
from datetime import datetime, timedelta 
import pandas as pd
import pytz

def main():
    try:
        rounded_minute = (datetime.now().minute // 15) * 15
        end_time = datetime.now().replace(minute=rounded_minute, second=0, microsecond=0)
        start_time = end_time - timedelta(minutes=15)

        
        
        queryset = db_std_data.objects.filter(time__range=(start_time, end_time)).values()
        data = list(queryset)
        df = pd.DataFrame(data)
        
        selected_cols1 = ['pm2_5_r','pm10_r','so2_nv','no2_nv','o3_nv','co_nv','device_id_id','time']
        selected_df1 = df[selected_cols1]
        
        avg_df = selected_df1.groupby(['device_id_id']).mean().reset_index()
        
        avg_df['start_time'] = df.groupby('device_id_id')['time'].transform('min')
        avg_df['end_time'] = df.groupby('device_id_id')['time'].transform('max')
        
        store_aggregated_data(avg_df)
        
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def store_aggregated_data(df):
    try:
        for _, row in df.iterrows():
            db_AirNet_Aggregated.objects.create(
                **row
            ).save()
    except Exception as e:
        print(f"An error occurred while storing aggregated data: {str(e)}")

if __name__ == "__main__":
    main()
