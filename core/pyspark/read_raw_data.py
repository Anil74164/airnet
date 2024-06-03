from pyspark.sql import SparkSession,Row
import pandas as pd
 
spark = SparkSession.builder \
        .appName("Django Spark Integration") \
        .getOrCreate()
        
     
# jdbc_url = "jdbc:postgresql://localhost:8000/airnetRaw"
 
# properties = {
#     "user": "vireshomkar",
#     "password": "host",
#     "driver": "org.postgresql.Driver"
# }
 
# def read_raw_data(jdbc_url, properties):
#     # Initialize Spark session
    
#     # Define the SQL query to read from raw_data table with WHERE condition
#     raw_data_query = "(SELECT * FROM RawData_db_aqdata_raw WHERE processed_dt IS NULL) AS raw_data_alias"
#     # Read data from raw_data table into a DataFrame with the WHERE condition
#     raw_data_df = spark.read.jdbc(url=jdbc_url, table=raw_data_query, properties=properties)
#     return raw_data_df
 
# # Define the JDBC URL and connection properties

# # Call the function to read data
# raw_data_df = read_raw_data(jdbc_url, properties)
 
# # Show the DataFrame
# raw_data_df.show()



import DjangoSetup
from RawData.models import db_AQData_Raw

raw=db_AQData_Raw.getRawData()
data = list(raw.values('timestamp', 'manufacturer', 'request_url', 'data', 'httpCode', 'isCalibrated', 'processed_dt'))
pdf = pd.DataFrame(data)

# print(df_pandas['data'])
# data=list(raw.values())
#print(data[0])
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, TimestampType,MapType
# schema = StructType([
#     StructField("timestamp", TimestampType(), True),
#     StructField("manufacturer", StringType(), True),
#     StructField("request_url", StringType(), True),
#     StructField("data", MapType(), True),  # Change StringType() to the appropriate type for JSONField
#     StructField("httpCode", StringType(), True),
#     StructField("isCalibrated", BooleanType(), True),
#     StructField("processed_dt", TimestampType(), True)
# ])
# rows = [Row(**record) for record in data]


# df = spark.createDataFrame(rows)
# df.show()



from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, MapType
 
def infer_schema(value):

    if isinstance(value, dict):

        return StructType([StructField(k, infer_schema(v), True) for k, v in value.items()])

    elif isinstance(value, list):

        if len(value) > 0:

            return ArrayType(infer_schema(value[0]))

        else:

            return ArrayType(StringType())

    elif isinstance(value, int):

        return IntegerType()

    elif isinstance(value, float):

        return FloatType()

    elif isinstance(value, str):

        return StringType()

    else:

        return StringType()
 
def pandas_to_spark_schema(pdf):

    schema = StructType()

    for column_name, value in pdf.iloc[0].items():

        schema.add(StructField(column_name, infer_schema(value), True))

    return schema


from pyspark.sql import SparkSession
 
# Initialize Spark session

spark = SparkSession.builder.appName("InferSchemaExample").getOrCreate()
 
# Infer the schema from the Pandas DataFrame

spark_schema = pandas_to_spark_schema(pdf)
 
# Convert the Pandas DataFrame to a Spark DataFrame using the inferred schema

sdf = spark.createDataFrame(pdf, schema=spark_schema)
 
# Show the Spark DataFrame

sdf.printSchema()

sdf.show(truncate=False)
