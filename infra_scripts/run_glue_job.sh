echo "Copying ${2} to s3://dev-glue-etl-data-raw/glue_resources/scripts/${1}/"
aws s3 cp ${2} s3://dev-glue-etl-data-raw/glue_resources/scripts/${1}/
echo "### Starting glue job run ####"
echo "Do you wish continue?"
select yn in "Yes" "No"; do
    case $yn in
        Yes ) aws glue start-job-run --job-name "bec-dev-test-migration-job" --region us-east-1; break;;
        No ) exit;;
    esac
done
# aws glue start-job-run --cli-input-json "$(< $3)"