import DjangoSetup
 
from core.models import db_std_data
import pandas as pd
import logging
from datetime import datetime,timedelta
import pytz
 
 
logger = logging.getLogger('workspace')
 
def fetch_std_data(start_date, end_date):
    try:
        # Ensure date format is correct
        print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        # start_date = datetime.strptime(start_date, "%Y%m%d%H%M%S")
        # end_date = datetime.strptime(end_date, "%Y%m%d%H%M%S")
 
        # Log the parsed dates
        # start_date='2024-06-12 15:15:27+05:30'
        # end_date='2024-06-12 15:30:27+05:30'
        # start_date = datetime.strptime(start_date, "%Y%m%d%H%M%S")
        # end_date = datetime.strptime(end_date, "%Y%m%d%H%M%S")
        # ist_timezone = pytz.timezone('Asia/Kolkata')
        # start_date=start_date.astimezone(ist_timezone)
        # end_date=end_date.astimezone(ist_timezone)
        # start_time=start_date
        # end_time=start_date
        
        ist_timezone = pytz.timezone('Asia/Kolkata')
        rounded_minute = (datetime.now().minute // 15) * 15
        # # end_time = datetime(2024,5,20,0,15,0)
        end_time = datetime.now().replace(minute=rounded_minute, second=0, microsecond=0)
        start_time = end_time - timedelta(minutes=15)
        # #start_time = end_time - timedelta(minutes=15)
        end_time=end_time.astimezone(ist_timezone)
        start_time=start_time.astimezone(ist_timezone)
        print(start_date,"  ",end_date)
        logger.info(f"Filtering data from {start_date} to {end_date}")
 
        # Fetch filtered data
        filtered_std_data = db_std_data.objects.filter(time__range=(start_time, end_time)).values()
        
        print("hhhhhhhhhhhhhhh")
        # Log the SQL query being executed
        #logger.info(f"SQL Query: {filtered_std_data.query}")
 
        # Convert queryset to list and then to DataFrame
        filtered_std_data_list = list(filtered_std_data)
        print(filtered_std_data_list)
        print("ffffffffffffff")
 
        # Log the raw data fetched from the database
        logger.info(f"Raw data fetched: {filtered_std_data_list}")
 
        # Convert to DataFrame
        std_data_df = pd.DataFrame(filtered_std_data_list)
 
        logger.info(f"Fetched {len(std_data_df)} records from db_std_data")
        return std_data_df
 
    except Exception as e:
        logger.error(f"Error in fetching data: {e}")
        return pd.DataFrame()
 
if __name__ == "__main__":
    # Example date range
    start_date = "20240612094500"    # Replace with your start date
    end_date = "20240612100000"
    naive_datetime = datetime.strptime(start_date,"%Y%m%d%H%M%S")
 
    # Create a timezone-aware datetime object with +5:30 offset (India Standard Time)
      # Adjust timezone as per your requirement
 
       # Replace with your end date
 
    std_data_df = fetch_std_data(start_date, end_date)
    print(std_data_df)