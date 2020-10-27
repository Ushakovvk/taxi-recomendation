
from pyspark.sql.types import *
from pyspark.sql.functions import *

def load_data(spark, path):
    
    schema_trips = StructType([StructField('VendorID',ByteType(),True),
                               StructField('lpep_pickup_datetime',TimestampType(),True),
                               StructField('lpep_dropoff_datetime',TimestampType(),True),
                               StructField('store_and_fwd_flag',StringType(),True),
                               StructField('RatecodeID',ByteType(),True),
                               StructField('PULocationID',IntegerType(),True),
                               StructField('DOLocationID',IntegerType(),True),
                               StructField('passenger_count',ByteType(),True),
                               StructField('trip_distance',FloatType(),True),
                               StructField('fare_amount',FloatType(),True),
                               StructField('extra',FloatType(),True),
                               StructField('mta_tax',FloatType(),True),
                               StructField('tip_amount',FloatType(),True),
                               StructField('tolls_amount',FloatType(),True),
                               StructField('ehail_fee',FloatType(),True),
                               StructField('improvement_surcharge',FloatType(),True),
                               StructField('total_amount',FloatType(),True),
                               StructField('payment_type',ByteType(),True),
                               StructField('trip_type',ByteType(),True),
                               StructField('congestion_surcharge',FloatType(),True)])
    
    df = spark.read \
        .format('csv') \
        .schema(schema_trips) \
        .option('header', 'true') \
        .option('delimiter', ',') \
        .load(path)
    
    return df
