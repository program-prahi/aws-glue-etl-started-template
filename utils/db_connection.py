import boto3
from botocore.exceptions import ClientError
import re
import json
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, String, DateTime,Float,Boolean
import pandas as pd
from utils.logger import Logger

log = Logger(__file__).get_logger()


def get_database_secret_string(secrets_name,region='us-west-2'):
    try:
        session = boto3.Session()
        secret_client = session.client('secretsmanager',region_name=region)
        secret_value = secret_client.get_secret_value(SecretId=secrets_name)['SecretString']
        return json.loads(secret_value)
    except ClientError as e:
        log.error("ErrorCode: {}, Received error: {}".format(e.response['Error']['Code'],e))
        raise e


# def get_redshift_connection_details(secrets_name):
#     secret_string = get_database_secret_string(secrets_name)
#     username = secret_string['username']
#     password = secret_string['password']
#     host = secret_string['host']
#     port = secret_string['port']
#     database = secret_string['dbname']
#     iam_role = secret_string['redshift_role_arn']
#     return username,password,host,port,database,iam_role

def get_mysql_server_connection_details(secrets_name):

    secret_string = get_database_secret_string(secrets_name)
    username = secret_string['username']
    password = secret_string['password']
    host = secret_string['host']
    port = secret_string['port']
    database = secret_string['dbname']
    return username,password,host,port,database


def get_dynamic_frame_from_catalog(glueContext,metadatadb,table,transformationname):

    dyf = glueContext.create_dynamic_frame.from_catalog(database=metadatadb, table_name = table, transformation_ctx = transformationname)
    return dyf


# def get_dynamic_frame_from_redshift(glueContext,secrets_name,table_name,temp_dir):
#     username, password, host, port, database, iam_role = get_redshift_connection_details(secrets_name)
#     jdbc_connection_string = f"jdbc:redshift://{host}:{port}/{database}"
#     connection_options = {  
#         "url": jdbc_connection_string,
#         "dbtable": table_name,
#         "user": username,
#         "password": password,
#         "redshiftTmpDir": temp_dir,
#         "aws_iam_role": iam_role
#     }
#     dyf = glueContext.create_dynamic_frame_from_options("redshift", connection_options)
#     return dyf


# def get_dynamic_from_redshift_query(spark_session,secrets_name,query):
#     log.info("===================== READING DATA FROM REDSHIFT DATBASE ==============")
#     username, password, host, port, database,iam_role = get_redshift_connection_details(secrets_name)
#     jdbc_connection_string = f"jdbc:redshift://{host}:{port}/{database}"
#     dyf = spark_session.read.format("jdbc") \
#                         .option("driver","com.amazon.redshift.jdbc42.Driver") \
#                         .option("url",jdbc_connection_string) \
#                         .option("user",username) \
#                         .option("password",password) \
#                         .option("dbtable",f"({query}) as data") \
#                         .load()
#     return dyf


def get_dynamic_from_mysql_connection_query(glueContext,secrets_name,query):
    log.info("===================== READING DATA FROM MYSQL DATBASE ==============")
    username,password,host,port,database = get_mysql_server_connection_details(secrets_name)
    jdbc_connection_string = f"jdbc:mysql://{host}:{port}/{database}"
    dyf = glueContext.spark_session.read.format("jdbc") \
                        .option("driver","com.mysql.cj.jdbc.Driver") \
                        .option("url",jdbc_connection_string) \
                        .option("user",username) \
                        .option("password",password) \
                        .option("dbtable",f"({query}) as data") \
                        .load()
    return dyf

def execute_transaction(engine,merge_sql_query):
    try:
        with engine.begin() as connection:
            log.info("<<<BEGIN TRANSACTION>>>")
            res = connection.execute(merge_sql_query)
            log.info("<<<END TRANSACTION>>>")
    except Exception as e:
        log.info("<<<Error in Transaction>>>")
        raise e


def write_jdbc_spark_session(dataFrame,secret_name, tableName, write_mode= 'append'):
    log.info("===================== Write DATA FROM MYSQL DATBASE ==============")
    username, password, host, port, database = get_mysql_server_connection_details(secret_name)
    jdbc_connection_string = f"jdbc:mysql://{host}:{port}/{database}"
    # Write dataframe to Database Table
    dataFrame.write \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", jdbc_connection_string) \
        .option("dbtable", f"{database}.{tableName}") \
        .option("user", username) \
        .option("password", password) \
        .save()

def write_dataframe_jdbc_table(dataFrame,target_table,jdbc_url,connection_properties,write_mode='append'):
    log.info(f"======= Writing to JDBC Table: {target_table} ======")
    dataFrame.write.jdbc(jdbc_url,
                        target_table,
                        mode=write_mode,
                        properties=connection_properties)


if __name__ == "__main__":
    # secrets_name = 'dev-apolloware/dev-mssql/secrets'
    secrets_name = "dev-apolloware/redshift/secrets"
    # username,password,host,port,database = get_sql_server_connection_details(secret_name)
    # engine = sa.create_engine(f'mssql+pymssql://{username}:{password}@{host}:{port}/{database}')
    # table_name = "TelemetryCalculationType"
    # sql_temp_schema = f"""select * from {table_name};"""
    # with engine.connect() as connection:
    #     res = connection.execute(sql_temp_schema)
    #     print(res.fetchall())
    username,password,host,port,database,iam_role = get_redshift_connection_details(secrets_name)
    jdbc_connection_string = f"jdbc:redshift://{host}:{port}/{database}"