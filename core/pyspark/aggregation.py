
import DjangoSetup
from core.models import db_std_data, db_AirNet_Aggregated
from datetime import datetime, timedelta 
import pandas as pd
import pytz

def main():
    try:
        ist_timezone = pytz.timezone('Asia/Kolkata')
        rounded_minute = (datetime.now().minute // 15) * 15
        end_time = datetime(2024,5,20,0,15,0)
        # end_time = datetime.now().replace(minute=rounded_minute, second=0, microsecond=0)
        start_time = end_time - timedelta(minutes=15)
        #start_time = end_time - timedelta(minutes=15)
        end_time=end_time.astimezone(ist_timezone)
        start_time=start_time.astimezone(ist_timezone)
        print(rounded_minute)
        
        
        print(start_time)
        print(end_time)
        
        
        queryset = db_std_data.objects.filter(time__range=(start_time, end_time)).values()
        data = list(queryset)
        print("aaaaaaaaaa")
        df = pd.DataFrame(data)
        print(df)
        
        if len(df)>0:
            
            # selected_df1 = df[selected_cols1]
            print(df.columns)
            print(df['time'])
            selected_df1 =df.drop(columns=['id','time'])
            counts = len(selected_df1)
            print(counts)
            avg_df = selected_df1.groupby(['device_id_id']).mean().reset_index()
        
            avg_df['start_time'] = df.groupby('device_id_id')['time'].transform('min')
            avg_df['end_time'] = df.groupby('device_id_id')['time'].transform('max')
            print(avg_df)
            if counts<=11:
                avg_df=avg_df[['start_time','end_time','device_id_id']]
                return
            
            avg_df['end_time']=avg_df['end_time'].dt.tz_localize("UTC")
            avg_df['start_time']=avg_df['start_time'].dt.tz_localize("UTC")
            avg_df['end_time']=avg_df['end_time'].dt.tz_convert("Asia/Kolkata")
            avg_df['start_time']=avg_df['start_time'].dt.tz_convert("Asia/Kolkata")
            avg_df['time']=start_time
            store_aggregated_data(avg_df)
            avg_df.to_csv("data4.csv")
            print(avg_df)
            
            print(avg_df.columns)

        else:
            print(f"There is no data for {start_time} and {end_time}")
        
    
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
