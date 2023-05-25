import json
from datetime import datetime, date
import boto3
import time

redshift = boto3.client("redshift-data")
# redshift_sm = boto3.client('secretsmanager')
secretNm = "secret-redshift-ducph"

# response = redshift_sm.get_secret_value(SecretId=secretNm)
# database_secrets = json.loads(response['SecretString'])
redshift_database = "dev"
redshift_cluster_id = "redshift-cluster-1"
redshift_user = "awsuser"
etl_date = str(date.today())


def lambda_handler(event, context):
    glue = boto3.client('glue')
    jobName = event["jobName"]
    lst_job_run_infor = []

    for job in jobName:
        rspStart = glue.start_job_run(JobName=job)
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        job_run_id = rspStart["JobRunId"]
        response = glue.get_job_run(JobName=job, RunId=job_run_id)
        status = response["JobRun"]["JobRunState"]
        sql = f"""begin transaction;
                insert into datn.etl_log values 
                ('{job}','{etl_date}','{job_run_id}',0,'{etl_date}','{start_time}','{start_time}','{status}');
                end transaction;"""
        print(sql)
        run = redshift.execute_statement(ClusterIdentifier=redshift_cluster_id, Database=redshift_database,
                                         SecretArn="arn:aws:secretsmanager:ap-southeast-1:039178755962:secret:secret-redshift-ducph-lqxhw5",
                                         Sql=sql)
        time.sleep(3)
        lst_job_run_infor.append([job, job_run_id, start_time])
    return lst_job_run_infor
