import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame 
from awsglue.transforms import Relationalize
from awsglue.job import Job
from pyspark.sql.types import StructType,StringType,StructField,LongType,DoubleType,BooleanType,DecimalType
from pyspark.sql.functions import *
import datetime
import boto3
import importlib

from utils import db_connection
from utils.logger import Logger


# Glue Context
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session



log = Logger(__file__).get_logger()
job = Job(glueContext)


# Configuration Variables - Crawler Catalog
output_database = None
output_table = None


log.info("Job initiated")
##Input Attributes
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                          'ENV',
                          'output_s3_dir'
                          ]
                          )

job.init(args['JOB_NAME'])


output_lg_partitioned_dir = args['output_s3_dir']

def get_time_dimension(start_year, end_year):

    end_year = str(int(end_year) + 1)
    df = spark.sql(
        f"SELECT sequence(to_timestamp('{start_year}'), to_timestamp('{end_year}'), interval 1 minute) as date") \
        .withColumn("date", explode(col("date"))) \
        .withColumn("datetime", col("date") + expr('INTERVAL 1 SECONDS')) \
        .withColumn("minutestartdatetime", col("date") + expr('INTERVAL 1 SECONDS')) \
        .withColumn("minuteenddatetime", col("date") + expr('INTERVAL 60 SECONDS')) \
        .withColumn("fiveminutesstartdatetime",
                    (floor(unix_timestamp(col("datetime")) / 300) * 300).cast("timestamp") + expr('INTERVAL 1 SECONDS')) \
        .withColumn("fiveminutesenddatetime", col("fiveminutesstartdatetime") + expr('INTERVAL 299 SECONDS')) \
        .withColumn("fifteenminutesstartdatetime",
                    (floor(unix_timestamp(col("datetime")) / 900) * 900).cast("timestamp") + expr('INTERVAL 1 SECONDS')) \
        .withColumn("fifteenminutesenddatetime", col("fifteenminutesstartdatetime") + expr('INTERVAL 899 SECONDS'))\
        .withColumn("hourstartdatetime", date_trunc("hour", col("datetime")) + expr('INTERVAL 1 SECONDS')) \
        .withColumn("hourenddatetime", col("hourstartdatetime") + expr('INTERVAL 3599 SECONDS')) \
        .withColumn("daystartdatetime", date_trunc("day", col("datetime")) + expr('INTERVAL 1 SECONDS')) \
        .withColumn("dayenddatetime", col("daystartdatetime") + expr('INTERVAL 86399 SECONDS')) \
        .withColumn("monthstartdatetime", date_trunc("month", col("datetime")) + expr('INTERVAL 1 SECONDS')) \
        .withColumn("monthenddatetime",
                    col("monthstartdatetime") + expr('INTERVAL 1 MONTHS') - expr('INTERVAL 1 SECONDS')) \
        .withColumn("yearstartdatetime", date_trunc("year", col("datetime")) + expr('INTERVAL 1 SECONDS')) \
        .withColumn("yearenddatetime", col("yearstartdatetime") + expr('INTERVAL 1 YEARS') - expr('INTERVAL 1 SECONDS')) \
        .withColumn("timedimensionid", (col("datetime").cast("integer"))) \
        .withColumn("minutestartdimensionid", (col("minutestartdatetime").cast("integer"))) \
        .withColumn("minuteenddimensionid", (col("minuteenddatetime").cast("integer"))) \
        .withColumn("fiveminutesstartdimensionid", (col("fiveminutesstartdatetime").cast("integer"))) \
        .withColumn("fiveminutesenddimensionid", (col("fiveminutesenddatetime").cast("integer"))) \
        .withColumn("fifteenminutesstartdimensionid", (col("fifteenminutesstartdatetime").cast("integer"))) \
        .withColumn("fifteenminutesenddimensionid", (col("fifteenminutesenddatetime").cast("integer"))) \
        .withColumn("hourstartdimensionid", (col("hourstartdatetime").cast("integer"))) \
        .withColumn("hourenddimensionid", (col("hourenddatetime").cast("integer"))) \
        .withColumn("daystartdimensionid", (col("daystartdatetime").cast("integer"))) \
        .withColumn("dayenddimensionid", (col("dayenddatetime").cast("integer"))) \
        .withColumn("monthstartdimensionid", (col("monthstartdatetime").cast("integer"))) \
        .withColumn("monthenddimensionid", (col("monthenddatetime").cast("integer"))) \
        .withColumn("yearstartdimensionid", (col("yearstartdatetime").cast("integer"))) \
        .withColumn("yearenddimensionid", (col("yearenddatetime").cast("integer"))) \
        .withColumn("year", year(col("datetime"))) \
        .withColumn("month", month(col("datetime"))) \
        .withColumn("day", dayofmonth(col("datetime"))) \
        .withColumn("hour", hour(col("datetime"))) \
        .withColumn("minute", minute(col("datetime"))) \
        .withColumn("second", second(col("datetime"))) \
        .drop(col("date")) \
        .filter(f"year <{end_year}")
    return df


log.info("Job running")
start_year = "2021"
end_year = "2022"

dataFrameDf = get_time_dimension(start_year, end_year)

# s3_path = output_lg_partitioned_dir + 'timedimension'
# dataFrameDf.coalesce(4).write.parquet(output_lg_partitioned_dir, partitionBy=['year','month'])
secret_name = "pcsg-big-data-mysql-secrets"
table_name = "timedimension2022"
db_connection.write_jdbc_spark_session(dataFrameDf.limit(1000), secret_name, table_name)

log.info("Job completed")