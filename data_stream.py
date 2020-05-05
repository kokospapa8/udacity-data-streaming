import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

schema = StructType([
    StructField('crime_id', StringType(), True),
    StructField('original_crime_type_name', StringType(), True),
    StructField('report_date', StringType(), True),
    StructField('call_date', StringType(), True),
    StructField('offense_date', StringType(), True),
    StructField('call_time', StringType(), True),
    StructField('call_date_time', TimestampType(), True),
    StructField('disposition', StringType(), True),
    StructField('address', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('agency_id', StringType(), True),
    StructField('address_type', StringType(), True),
    StructField('common_location', StringType(), True),
])

def run_spark_job(spark):


    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers','localhost:9092') \
        .option('subscribe', 'service.call.police') \
        .option('startingOffsets', 'earliest') \
        .option('maxOffsetsPerTrigger', 10) \
        .option('maxRatePerPartition', 10) \
        .option('stopGracefullyOnShutdown', "true") \
        .load()


    df.printSchema()

    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    service_table.printSchema()
    
    distinct_table = service_table \
            .selectExpr('original_crime_type_name', 
                        'disposition', 
                        'to_timestamp(call_date_time) as call_date_time') \
            .withWatermark("call_date_time", "60 minutes") 
    
    # count the number of original crime type
    agg_df = distinct_table\
        .withWatermark("call_date_time", "60 minutes") \
        .groupBy(psf.window(distinct_table.call_date_time, "60 minutes", "10 minutes"), 
                 distinct_table.original_crime_type_name) \
        .count() \
        .selectExpr('original_crime_type_name', 
                    'count', 
                    'window.start as w_s', 
                    'window.end as w_e')
    
    query_df = distinct_table.join(
        agg_df, \
            (distinct_table.call_date_time >= agg_df.w_s)  \
            & (distinct_table.call_date_time < agg_df.w_e) \
            & (distinct_table.original_crime_type_name 
             == agg_df.original_crime_type_name) , "inner")

    query = agg_df \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .trigger(processingTime="3 seconds")\
            .start()
    query.awaitTermination()

    
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    join_df = agg_df \
        .join(radio_code_df, "disposition")
        
    join_query = join_df\
        .writeStream \
        .format("console") \
        .queryName("join") \
        .start()

    join_query.awaitTermination()
    

if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port", 3000) \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()

#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] --conf spark.ui.port=3000 data_stream.py
