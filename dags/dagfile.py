from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime, timedelta
import os
 
 #Define the Python function to execute the PySpark job
def Data_Ingestion_job():
     # Execute the PySpark script using spark-submit
     os.system("spark-submit --master local[*] ./core/pyspark/DataIngestion.py")
 
# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}
 
# Create a DAG object
dag = DAG(
    'Data_Ingestion_job',
    default_args=default_args,
    description='A DAG to fetch response from sensors into raw schema',
    schedule_interval=timedelta(minutes=1),  # Run 1 minute
)
 
# Create a PythonOperator to execute the job
execute_pyspark_job_task = PythonOperator(
    task_id='Data_Ingestion_job',
    python_callable=Data_Ingestion_job,
    dag=dag,
)
