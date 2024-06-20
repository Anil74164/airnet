import subprocess
import logging
from datetime import datetime
import DjangoSetup
from core.models import db_missing_data


# Setup logger
logger = logging.getLogger('workspace')

try:
    # Fetch all data from db_missing_data where status is 0
    all_missing_data = db_missing_data.objects.filter(status=0)
    data = list(all_missing_data)  # Convert queryset to list
    print(data)
    
    # Iterate through each missing data record
    for i in data:
        start_time = i.req_start_dt.strftime('%Y%m%d%H%M%S')
        end_time = i.req_end_dt.strftime('%Y%m%d%H%M%S')
        device_id = i.device_id_id
        parameter = i.parameter
        
        # Prepare the command to execute DataIngestion.py with specific parameters
        cmd = [
            'spark-submit', 'core/pyspark/DataIngestion.py',
            '-s', start_time,
            '-e', end_time,
            '-d', str(device_id),
            '-p', parameter
        ]
        
        # Log the command for debugging
        # print(f"Executing command: {cmd}")
        
        # Execute the command and capture the result
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        # Check the result and log accordingly
        if result.returncode == 0:
            print(f"Fetched data for device {device_id} with parameter {parameter} "
                  f"from {start_time} to {end_time}")
            
            logger.info(f"Fetched data for device {device_id} with parameter {parameter} "
                        f"from {start_time} to {end_time}")
            
        else:
            print(f"Error fetching data for device {device_id} with parameter {parameter} "
                  f"from {start_time} to {end_time}: {result.stderr}")
            logger.error(f"Error fetching data for device {device_id} with parameter {parameter} "
                         f"from {start_time} to {end_time}: {result.stderr}")

except Exception as e:
    logger.error("An error occurred: %s", str(e))