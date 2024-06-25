
import DjangoSetup
from core.models import db_std_data, db_AirNet_Aggregated
from datetime import datetime, timedelta 
import pandas as pd
import pytz
from core.pyspark.common import *

def main(args=None):
    try:
        if args.start and args.end:
 
            # ist_timezone = pytz.timezone("Asia/Kolkata")
            # start= ist_timezone.localize(args.start)
            # start = start.astimezone(pytz.)
            # end= ist_timezone.localize(args.end)
            # end = end.astimezone(pytz.utc)
            
            start=args.start
            end=args.end
            print(type(start))
            print(start)
        else:
            end = datetime.now().replace(minute=(datetime.now().minute // 15) * 15, second=0, microsecond=0)
            start = end - timedelta(minutes=15)
            
            ist_timezone = pytz.timezone("Asia/Kolkata")
            start = start.astimezone(ist_timezone)
            end = end.astimezone(ist_timezone)
            print(start," ",end)
        
        
        queryset = db_std_data.objects.filter(time__range=(start, end)).values()
        data = list(queryset)
    
        df = pd.DataFrame(data)
        print(df)
        
        if len(df)>0:
            
            # selected_df1 = df[selected_cols1]
         
            selected_df1 =df.drop(columns=['id','time'])
            counts = len(selected_df1)
   
            avg_df = selected_df1.groupby(['device_id_id']).mean().reset_index()
        
            avg_df['start_time'] = df.groupby('device_id_id')['time'].transform('min')
            avg_df['end_time'] = df.groupby('device_id_id')['time'].transform('max')
            if counts<=11:
                avg_df=avg_df[['start_time','end_time','device_id_id']]
                return
            
            avg_df['end_time']=avg_df['end_time'].dt.tz_localize("UTC")
            avg_df['start_time']=avg_df['start_time'].dt.tz_localize("UTC")
            avg_df['end_time']=avg_df['end_time'].dt.tz_convert("Asia/Kolkata")
            avg_df['start_time']=avg_df['start_time'].dt.tz_convert("Asia/Kolkata")
            avg_df['time']=start
            store_aggregated_data(avg_df)
            avg_df.to_csv("data4.csv")
            print(avg_df)
          

        else:
            print(f"There is no data for {start} and {end}")
        
    
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
    spark_init()
    # test_args = [
    #         'DataIngestion.py'
    #         '-d', '1210230117',
    #         '-d', '1210230136',
    #         '-p', 'pm10_base',
    #     ]
    
    # sys.argv = test_args

    args = get_options()
 
    main( args )
    
