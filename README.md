# Glue Sample ETL

  - This Project repository is for sample Glue ETL operations run with JDBC connections
  - The ETL Operations (Extract, Transform, Load) are performed using [Spark-2.4.3](https://spark.apache.org/docs/2.4.3/index.html) available under the Managed ETL Service by AWS under [AWS Glue](https://docs.aws.amazon.com/en_us/glue/latest/dg/add-job.html)
  - The project uses [AWS Glue v2.0](https://docs.aws.amazon.com/glue/latest/dg/reduced-start-times-spark-etl-jobs.html) Spark Jobs written in Python 3.7(Pyspark)
  - The base paradigm is kept largely maintained functional instead of OOPS.
  - Create an Glue Service IAM role for Glue Job execution

## Zip your extra python module
    -  Prerequisites: zip or 7zip
    -  Upload the .zip file to a spefic S3 bucket path and reference S3 path as `--extra-py-files` argument in glue job 
```
zip -r glue-supporting-utils.zip ./utils -x '*__pycache__*'
```
## Infra scripts
  
  - [create_job.sh](./infra_scripts/create_job.sh)
    - Creates a Glue job based on the cli-input-json available in [infra_scripts/jobs](./infra_scripts/jobs) directory. See Usage details in script.
  
  - [run_glue_job.sh](./infra_scripts/run_glue_job.sh)
    - Runs the glue job. See Usage details in script.

## ETL Glue Scripts
  - [main.py](./main.py)
    - Generates a timeseries table and write table to S3 and JDBC as desired 
    - Job Arguments:
    ```
    "--ENV": "dev",
    "--output_s3_dir": "s3a://pcsg-glue-code-base/output_files/"
    ```
    ```
    - [Job Run Details](https://docs.amazonaws.cn/en_us/glue/latest/dg/aws-glue-api-jobs-runs.html)
      - DPU Allocated: 2 
      - Worker Type: Standard
      - SecurityConfiguration: SSE-KMS Enabled for Amazon S3 and Amazon Cloudwatch
      - Extra Python Library: [requirements.txt](./requirements.txt)

## Database Connection Establishments
  - [db_connection.py](./apolloware_utils/db_connection.py)
    - Contains relevent helper functions related to pulling tables from Databases using JDBC Connection

## Logger Utility
  - [logger.py](./utils/logger.py)
    - Uses PyPI `logging` as per PEP 282