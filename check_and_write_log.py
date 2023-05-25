import time
import boto3
import json
from datetime import datetime, date

etl_date = str(date.today())

glue = boto3.client('glue')
redshift = boto3.client("redshift-data")
# redshift_sm = boto3.client('secretsmanager')
secretNm = "secret-redshift-ducph"

# response = redshift_sm.get_secret_value(SecretId=secretNm)
# database_secrets = json.loads(response['SecretString'])
redshift_database = "dev"
redshift_cluster_id = "redshift-cluster-1"
redshift_user = "awsuser"


# def run_job(jobName):
#     rspStart = glue.start_job_run(JobName = jobName)
#     start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#     job_run_id = rspStart["JobRunId"]

#     return jobName,job_run_id,start_time

def check_status_and_write_log(jobName, job_run_id, start_time):
    response = glue.get_job_run(JobName=jobName, RunId=job_run_id)
    status = response["JobRun"]["JobRunState"]
    print(status + jobName)

    end = ["FAILED", "ERROR", "TIMEOUT"]
    end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if status == "STARTING":
        print(status)
        return 0
    elif status in end:
        sql = f"""begin transaction;
            delete from datn.etl_log where job_nm='{jobName}' and etl_dt='{etl_date}';
            insert into datn.etl_log values 
            ('{jobName}','{etl_date}','{job_run_id}',-1,'{etl_date}','{start_time}','{end_time}','{status}');
            end transaction;"""
        run = redshift.execute_statement(ClusterIdentifier=redshift_cluster_id, Database=redshift_database,
                                         SecretArn="arn:aws:secretsmanager:ap-southeast-1:039178755962:secret:secret-redshift-ducph-lqxhw5",
                                         Sql=sql)
        time.sleep(3)
        print(status)
        return 1
    elif status == "RUNNING":

        sql = f"""begin transaction;
            delete from datn.etl_log where job_nm='{jobName}' and etl_dt='{etl_date}';
            insert into datn.etl_log values ('{jobName}','{etl_date}','{job_run_id}',0,'{etl_date}','{start_time}','{end_time}','{status}');
            end transaction;"""
        run = redshift.execute_statement(ClusterIdentifier=redshift_cluster_id, Database=redshift_database,
                                         SecretArn="arn:aws:secretsmanager:ap-southeast-1:039178755962:secret:secret-redshift-ducph-lqxhw5",
                                         Sql=sql)
        time.sleep(3)
        print(status)
        return 0
    elif status in ["STOPPING", "STOPPED", "SUCCEEDED"]:
        sql = f"""begin transaction;
            delete from datn.etl_log where job_nm='{jobName}' and etl_dt='{etl_date}';
            insert into datn.etl_log values 
            ('{jobName}','{etl_date}','{job_run_id}',1,'{etl_date}','{start_time}','{end_time}','{status}');
            end transaction;"""
        run = redshift.execute_statement(ClusterIdentifier=redshift_cluster_id, Database=redshift_database,
                                         SecretArn="arn:aws:secretsmanager:ap-southeast-1:039178755962:secret:secret-redshift-ducph-lqxhw5",
                                         Sql=sql)
        time.sleep(3)
        print(sql)
        return 1


def lambda_handler(event, context):
    d = 0
    lst_job_run_infor = event["lst_job_run_infor"]
    print(lst_job_run_infor)
    for job_run_infor in lst_job_run_infor:
        jobName = job_run_infor[0]
        job_run_id = job_run_infor[1]
        start_time = job_run_infor[2]
        run = check_status_and_write_log(jobName, job_run_id, start_time)
        print(run)
        d = d + run
    if d == len(event["lst_job_run_infor"]):
        return 1
    return 0
