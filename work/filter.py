
from pyspark.sql.functions import *

def filter_data(df):
    
    drop_cols = ['VendorID', 'store_and_fwd_flag', 'extra', 'mta_tax', 'ehail_fee', 'improvement_surcharge','congestion_surcharge']
    df = df.drop(*drop_cols)

    diff_secs_col = col("lpep_dropoff_datetime").cast("long") - col("lpep_pickup_datetime").cast("long")
    df = df.withColumn("duration", (diff_secs_col / 60).cast('int')) \
        .withColumn("speed", col('trip_distance') / (col('duration') / 60)) \
        .withColumn('date_file', date_trunc('month', to_timestamp(regexp_extract(input_file_name(), r'(\d+\-\d+)', 1), 'yyyy-MM')))

    
    df = df.filter('fare_amount >= 3').filter('fare_amount < 150')
    df = df.filter('speed > 0.5').filter('speed < 100')
    df = df.filter('duration > 1').filter('duration < 150')
    df = df.filter('trip_distance > 0.1').filter('trip_distance < 150')
    df = df.filter('passenger_count > 0')
    df = df.filter('payment_type < 3')
    df = df.filter('RatecodeID < 5')
    df = df.filter(date_trunc('month', col('lpep_pickup_datetime')) == df['date_file']).drop('date_file')
    df = df.filter('lpep_pickup_datetime < lpep_dropoff_datetime')
    df = df.dropna()
    
    #select_cols = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID','total_amount']
    #df = df.select(select_cols)
    
    return df
