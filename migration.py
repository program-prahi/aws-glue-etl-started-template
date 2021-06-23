#############
####   Data Migration Template: From Redshift to MSSQL Interim
#####  Ref: Glue resources in s3 bucket for libs and jars: s3://{env}-apolloware-data-raw/glue_resources/
#############
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
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
import datetime
import boto3
import importlib
from apolloware_utils import db_connection
from apolloware_utils.scripts import mapping_tables
from apolloware_utils.scripts import common
from apolloware_utils.logger import Logger
from apolloware_utils.helper_functions import convert_cst_local

#### Setting Spark Decimal Precision to avoid Data Loss
#### See Open Issue In Spark: https://issues.apache.org/jira/browse/SPARK-27089
# conf = SparkConf().setAll([('spark.sql.decimalOperations.allowPrecisionLoss','false')])
# sc = SparkContext(conf=conf)
#Create Glue Context and Spark Session
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
# sqlContext = SQLContext(SparkContext.getOrCreate())
# sqlContext.setConf("spark.sql.decimalOperations.allowPrecisionLoss","false")
# spark.conf.set("spark.sql.session.timeZone", "UTC")

log = Logger(__file__).get_logger()
job = Job(glueContext)
# Configuration Variables - Crawler Catalog
input_database = None
input_table = None

# Clear files in Output bucket
output_bucket = None
output_path = None
operation = None
file_filter = "run"
telemetry_module_name =  "apolloware_utils.scripts."
telemetry = None
      
##Input Attributes
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                          'TempDir',
                          'ENV',
                          'AWS_REGION',
                          'REDSHIFT_SECRETS_NAME',
                          'MS_SQL_SECRETS_NAME',
                          'MY_SQL_SECRETS_NAME',
                          'output_bucket',
                          'output_path',
                          'redshift_glue_connection',
                          'type_of_telemetry'])
job.init(args['JOB_NAME'], args)
logObj = Logger(args['JOB_NAME'])
logObj.logJobStart()

def get_tfh_table_from_mappers(MAPPING_SQL_QUERY):
    
    mapping_tfh_df = spark.sql(MAPPING_SQL_QUERY)
    mapping_tfh_df = (mapping_tfh_df
                                .withColumn("ReportStartLocal",udf_convert_to_cst(F.col("IntervalStart")))
                                .withColumn("ReportEndLocal",udf_convert_to_cst(F.col("IntervalEnd")))
                                .withColumn("InsertDateTime",F.current_timestamp())
                                .select(*mapping_tables.TFH_COLUMNS))
    return mapping_tfh_df 

def get_tif_table_from_mappers(MAPPING_SQL_QUERY):
    
    mapping_tif_df = spark.sql(MAPPING_SQL_QUERY)
    mapping_tif_df = (mapping_tif_df
                                .withColumn("Dirty",F.lit(0).cast("boolean"))
                                .withColumn("InsertDateTime",F.current_timestamp())
                                .select(*mapping_tables.TIF_COLUMNS))
    return mapping_tif_df 

# if ('input_database' not in args or 'input_table' not in args or 'output_bucket' not in args or 'output_path' not in args or 'operation' not in args
#     or args['input_database'] is None or args['input_table'] is None or args['output_bucket'] is None or args['output_path'] is None or args['operation'] is None
#     or args['input_database'] == '' or args['input_table'] == '' or args['output_bucket'] == '' or args['output_path'] == '' or args['operation'] == ''): 
#     #write out an error message and exit
#     logger.error('An input parameter was not passed in correctly')
#     missingInput = ''
#     if args['input_database'] is None:
#         missingInput = 'input_database'
#     if args['input_table'] is None: 
#         missingInput = 'input_table'
#     if args['output_bucket'] is None:
#         missingInput = 'output_bucket'
#     if args['output_path'] is None:
#         missingInput = 'output_path'
#     if args['operation'] is None:
#         missingInput = 'operation'
#     logger.error('** The input Variable ' + missingInput + ' is not present in the input for Job: ' + args['JOB_NAME'])
#     sys.exit(1)

ENV = args['ENV']
REDSHIFT_SECRETS_NAME = args['REDSHIFT_SECRETS_NAME']
MS_SQL_SECRETS_NAME = args['MS_SQL_SECRETS_NAME']
MY_SQL_SECRETS_NAME = args['MY_SQL_SECRETS_NAME']
AWS_REGION = args['AWS_REGION']
redshift_glue_connection = args['redshift_glue_connection']
output_bucket = args['output_bucket']
output_path = args['output_path']
type_of_telemetry = args['type_of_telemetry']
LIMIT_RECORDS = 10
## Bandera JIRA Issue: https://banderaelectric.atlassian.net/browse/A30-1175
udf_convert_to_cst = udf(convert_cst_local)
# s3 = boto3.resource('s3')
# bucket = s3.Bucket(output_bucket)

# now = datetime.datetime.utcnow()
# logger.info(f"Job Run: {now}")
# for obj in bucket.objects.filter(Prefix=output_path + "/" + file_filter):
#     s3.Object(bucket.name, obj.key).delete()

import pkg_resources
log.info([p.project_name for p in pkg_resources.working_set])

try:
    dynamically_import_module_name = telemetry_module_name + type_of_telemetry
    telemetry = importlib.import_module(dynamically_import_module_name)
except ModuleNotFoundError as error:
    log.error(f"Unable to find module: {dynamically_import_module_name}")
    raise error
# note that this is same as calling pip._vendor.pkg_resources.working_sets
## Getting Base Fact table of Power Summary from Redshift
base_fact_table_df = db_connection.get_dynamic_from_redshift_query(spark,REDSHIFT_SECRETS_NAME,telemetry.UTILITY_FACT_TABLE_QUERY)
# log.info(f"==========Limit of Fact table Records:{LIMIT_RECORDS}======================")
# base_fact_table_df = base_fact_table_df.limit(LIMIT_RECORDS)
# base_fact_table_df.printSchema()
base_fact_table_name = "telemetry" + type_of_telemetry + "fact"
base_fact_table_df.createOrReplaceTempView(base_fact_table_name)
# spark.sql(f"SELECT sitetypeid,intervalstart,intervalend,batchinsertedtime,count(1) FROM {base_fact_table_name} GROUP BY sitetypeid,intervalstart,intervalend,batchinsertedtime").show(truncate=False)
## Mark Batch as Processed
log.info(f"Selecting Distinct batches of Table: {base_fact_table_name}")
BATCH_PROCESS_QUERY = f"""
    SELECT DISTINCT
        batchinsertedtime,
        to_date(batchinsertedtime) AS batchdate,
        1 AS isprocessed
    FROM {base_fact_table_name}
"""
batch_process_df = spark.sql(BATCH_PROCESS_QUERY)
batch_process_df.show(truncate=False)
batch_process_df.count()
batch_process_df.cache()

base_table_unpivot_df = spark.sql(telemetry.UTILITY_FACT_TABLE_UNPIVOT_QUERY)

log.info("Setting Spark SQL to allow Precision with Decimal(32,6)")
base_table_unpivot_df = base_table_unpivot_df.withColumn("Value",F.col("Value").cast(DecimalType(32,6)))
fact_unfiltered_view_name = type_of_telemetry + "TelmetryUnfiltered"
base_table_unpivot_df.createOrReplaceTempView(fact_unfiltered_view_name)

## typecodedimension table
typecodedimension_df = db_connection.get_dynamic_from_redshift_query(spark,REDSHIFT_SECRETS_NAME,common.TYPECODE_DIMENSION_TABLE_QUERY)
typecodedimension_df.createOrReplaceTempView("typecodedimension")

## Typecode filtering for TIF
typecode_filter_query_tif = common.TYPECODE_FILTER_TIF_Q.format(fact_unfiltered_view_name)

## Typecode filtering for TFH
typecode_filter_query_tfh = common.TYPECODE_FILTER_TFH_Q.format(fact_unfiltered_view_name)

### TFH
telemetry_tfh_view_name = type_of_telemetry + "TFHTelemetry"
ps_unpivot_tfh_df = spark.sql(typecode_filter_query_tfh)
ps_unpivot_tfh_df.createOrReplaceTempView(telemetry_tfh_view_name)
ps_unpivot_tfh_df.printSchema()

### TIF
telemetry_tif_view_name = type_of_telemetry + "TIFTelemetry"
ps_unpivot_tif_df = spark.sql(typecode_filter_query_tif)
ps_unpivot_tif_df.createOrReplaceTempView(telemetry_tif_view_name)
ps_unpivot_tif_df.printSchema()

batch_process_df.count()
batch_process_df.show()

### Utility Mapping table

utility_mapping_df = db_connection.get_dynamic_from_redshift_query(spark,REDSHIFT_SECRETS_NAME,mapping_tables.UTILITY_MAPPING_QUERY)
utility_mapping_df.createOrReplaceTempView("interimUtilityMapping")
utility_mapping_df.cache()

### Substation Mapping Table

substation_mapping_df = db_connection.get_dynamic_from_redshift_query(spark,REDSHIFT_SECRETS_NAME,mapping_tables.SUBSTATION_MAPPING_QUERY)
substation_mapping_df.createOrReplaceTempView("interimSubstationMapping")
substation_mapping_df.cache()
substation_mapping_df.count()
## SLOC Mapping Table for TFH

sloc_mapping_df = db_connection.get_dynamic_from_redshift_query(spark,REDSHIFT_SECRETS_NAME,mapping_tables.SLOC_MAPPING_TFH_QUERY)
sloc_mapping_df.createOrReplaceTempView("interimSLOCMappingTFH")
sloc_mapping_df.cache()
sloc_mapping_df.count()
## SLOC Mapping Table for TIF

sloc_mapping_tif_df = db_connection.get_dynamic_from_redshift_query(spark,REDSHIFT_SECRETS_NAME,mapping_tables.SLOC_MAPPING_TIF_QUERY)
sloc_mapping_tif_df.createOrReplaceTempView("interimSLOCMappingTIF")
sloc_mapping_tif_df.cache()
sloc_mapping_tif_df.count()

batch_process_df.count()
batch_process_df.show()
## Create ServiceLocation/Org Level Mapping

## Pass Table names to the Mapping Template to get SQL Query of Each Mapping Table
PS_UTILITY_MAPPING_TFH_Q = mapping_tables.MAPPING_QUERY_TEMPLATE_TFH.format(telemetry_tfh_view_name,"interimUtilityMapping")
PS_SUBSTATION_MAPPING_TFH_Q = mapping_tables.MAPPING_QUERY_TEMPLATE_TFH.format(telemetry_tfh_view_name,"interimSubstationMapping")
PS_SLOC_MAPPING_TFH_Q = mapping_tables.MAPPING_QUERY_TEMPLATE_TFH.format(telemetry_tfh_view_name,"interimSLOCMappingTFH")
PS_SLOC_MAPPING_TIF_Q = mapping_tables.MAPPING_QUERY_TEMPLATE_TIF.format(telemetry_tif_view_name,"interimSLOCMappingTIF")


## Build TFH Dataframe Table for Each Mapping Table
ps_utility_tfh_df = get_tfh_table_from_mappers(PS_UTILITY_MAPPING_TFH_Q)
ps_substation_tfh_df = get_tfh_table_from_mappers(PS_SUBSTATION_MAPPING_TFH_Q)
ps_sloc_tfh_df = get_tfh_table_from_mappers(PS_SLOC_MAPPING_TFH_Q)

## Build TIF Dataframe Table from Mapping Table SLOC
ps_sloc_tif_df = get_tif_table_from_mappers(PS_SLOC_MAPPING_TIF_Q)
ps_sloc_tif_df.printSchema()
# ps_sloc_tif_df.show(truncate=False)
# ps_sloc_tif_df.createOrReplaceTempView("tif_sloc")
# spark.sql("SELECT IntervalStart,IntervalEnd,count(1) FROM tif_sloc GROUP BY IntervalStart,IntervalEnd").show(truncate=False)


## Merge to TIF Table
log.info("-------------DELETE AND INSERT-TIF----------: ps_sloc_tif_df")
db_connection.merge_tif_table(ps_sloc_tif_df,MS_SQL_SECRETS_NAME,type_of_telemetry)

## Merge to TFH table
log.info("-------------DELETE AND INSERT-TFH----------: ps_utility_df")
db_connection.merge_tfh_table(ps_utility_tfh_df,MS_SQL_SECRETS_NAME,type_of_telemetry)
log.info("-------------DELETE AND INSERT-TFH----------: ps_substation_df")
db_connection.merge_tfh_table(ps_substation_tfh_df,MS_SQL_SECRETS_NAME,type_of_telemetry)
log.info("-------------DELETE AND INSERT-TFH----------: ps_sloc_df")
db_connection.merge_tfh_table(ps_sloc_tfh_df,MS_SQL_SECRETS_NAME,type_of_telemetry)

## Mark Batch as Processed
log.info("================= BATCH PROCESS INSERT =============")
batch_process_df.count()
batch_process_df.show()
target_batch_process_table = type_of_telemetry + 'batchmetadata_interim_reroute'
log.info("-------------INSERT-Redshift----------: batch_process_df")
db_connection.mark_batch_as_processed(spark,REDSHIFT_SECRETS_NAME,batch_process_df,target_batch_process_table)

# unpivoted_DF_precision.printSchema()
# unpivoted_DF_precision.show(truncate=False)
logObj.logJobEnd()   
job.commit()