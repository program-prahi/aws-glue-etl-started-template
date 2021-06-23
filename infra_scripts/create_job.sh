#!/bin/bash
SCRIPT_NAME=`basename $0`
if [ "$#" -lt 1 ]; then
  echo "Usage: ${SCRIPT_NAME} <ENV_NAME> <SCRIPT_PATH> <JOB_PARAMS> <REGION>(opt)"
  exit 0
fi
cur_dir=`pwd`
pwd

ENV=${1}
AWS_REGION=${4:-'us-east-1'}
echo "Copying ${1} to s3"
aws s3 cp ${2} s3://pcsg-glue-code-base/
echo "### Creating glue job run ####"
echo "Do you wish to create Glue Job?"

select yn in "Yes" "No"; do
    case $yn in
        Yes ) job_id=`aws glue create-job --cli-input-json "$(< $3)" --region ${AWS_REGION}| jq -r .Name`;break;;
        No ) exit;;
    esac
done
echo ${job_id}
