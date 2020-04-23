import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# done TODO Create a schema for incoming resources
'''
Sample of input
 crime_id:"183653763"
original_crime_type_name:"Traffic Stop"
report_date:"2018-12-31T00:00:00.000"
call_date:"2018-12-31T00:00:00.000"
offense_date:"2018-12-31T00:00:00.000"
call_time:"23:57"
call_date_time:"2018-12-31T23:57:00.000"
disposition:"ADM"
address:"Geary Bl/divisadero St"
city:"San Francisco"
state:"CA"
agency_id:"1"
address_type:"Intersection"
common_location:"" 
'''


schema = StructType([StructField("crime_id", StringType(), False),
                     StructField("original_crime_type_name", StringType(), False),
                     StructField("report_date", StringType(), True),
                     StructField("call_date", StringType(), True),
                     StructField("offense_date", StringType(), True),
                     StructField("call_time", StringType(), True),
                     StructField("call_date_time", StringType(), True),
                     StructField("disposition", StringType(), True),
                     StructField("address", StringType(), True),
                     StructField("city", StringType(), True),
                     StructField("state", StringType(), True),
                     StructField("agency_id", StringType(), True),
                     StructField("address_type", StringType(), True),
                     StructField("common_location", StringType(), True)
                     ])



def run_spark_job(spark):

    # done TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "police.received.calls") \
        .option("startingOffsets", "earliest") \
        .option("maxRatePerPartition", 200) \
        .option("maxOffsetPerTrigger", 200) \
        .load()


    # Show schema for the incoming resources for checks
    df.printSchema()

    # done TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # done TODO select original_crime_type_name and disposition
    distinct_table = service_table.select(psf.col("original_crime_type_name"),
                                          psf.col("disposition"),
                                          psf.to_timestamp(psf.col("call_date_time")).alias("call_date_time")
                                         )

    # count the number of original crime type
    agg_df = distinct_table.select(psf.col("call_date_time"), psf.col("original_crime_type_name"), psf.col("disposition")) \
                        .withWatermark("call_date_time", "60 minutes") \
                        .groupBy( psf.window(distinct_table.call_date_time, "10 minutes", "5 minutes"), psf.col("original_crime_type_name")) \
                        .count()
    
    agg_df.printSchema()

    # done TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # done TODO write output stream
    query =  agg_df \
        .writeStream \
        .format("console") \
        .outputMode("complete") \
        .start()
    
    # done TODO attach a ProgressReporter
    query.awaitTermination()

    # done TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # done TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # done TODO join on disposition column
    join_query = agg_df \
        .join(radio_code_df, "disposition") \
        .writeStream \
        .format("console") \
        .queryName("join") \
        .start()


    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # done TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000) \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
