
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os
 
# Define the timeit decorator
def timeit(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        start_time_str = time.strftime('%Y%m%d:%H%M%S', time.localtime(start))
        result = func(*args, **kwargs)
        end = time.time()
        print(f"{start_time_str} {func.__name__} took {str((end - start) * 1000)} milliseconds")
        return result
    return wrapper
 
@timeit
def execute_spark_program(**kwargs):
    try:
        now = datetime.now()
        rounded_minute = (now.minute // 15) * 15
        endDt = now.replace(minute=rounded_minute, second=0, microsecond=0)
        startDt = endDt - timedelta(minutes=15)
        
        startDt_str = startDt.strftime('%Y%m%d%H%M%S')
        endDt_str = endDt.strftime('%Y%m%d%H%M%S')
        
        command = f"spark-submit --master local[*] ./core/pyspark/DataIngestion.py -s {startDt_str} -e {endDt_str}"
        result = os.system(command)
        
        if result != 0:
            print(f"Command failed with exit code {result}")
        else:
            print("Command executed successfully")
    except OSError as e:
        print(f"OSError: {e.strerror}")
 
@timeit
def aggregation(**kwargs):
    try:
        command = "spark-submit --master local[*] ./core/pyspark/aggregation.py"
        result = os.system(command)
        
        if result != 0:
            print(f"Command failed with exit code {result}")
        else:
            print("Command executed successfully")
    except OSError as e:
        print(f"OSError: {e.strerror}")
 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': (datetime.today() - timedelta(days=1)),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}
 
dag = DAG(
    'main_data_processing_dag',
    default_args=default_args,
    description='A DAG for data processing',
    schedule_interval=timedelta(minutes=15),  # Run every 15 minute
)
 
# Task to execute the PySpark job
execute_pyspark_job_task = PythonOperator(
    task_id='execute_pyspark_job',
    python_callable=execute_spark_program,
    dag=dag,
)
 
aggregation_task = PythonOperator(
        task_id='aggregation',
        python_callable=aggregation,
        dag=dag,
)
execute_pyspark_job_task >> aggregation_task