import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime,date
from awsglue.dynamicframe import DynamicFrame
from boto3.dynamodb.conditions import Key, Attr
import boto3
import json

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME',"secretNameDwh"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

secretNameDwh = args["secretNameDwh"]

def getCredentials(secretname):
    credential = {}
    
    secret_name = secretname
    region_name = "ap-southeast-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    get_secret_value_response = client.get_secret_value(
      SecretId=secret_name
    )
    secret = json.loads(get_secret_value_response['SecretString'])
    credential['username'] = secret['username']
    credential['password'] = secret['password']
    credential['port'] = secret['port']
    credential['host'] = secret['host']
    
    if "url" in secret:
        credential['url'] = secret['url']
    else:
        credential['url'] = ""
        
    #Dwh
    if "s3bucket" in secret:
        credential['s3bucket'] = secret['s3bucket']
    
    if "catalogdb" in secret:
        credential['catalogdb'] = secret['catalogdb']
        
    if "dbInstanceIdentifier" in secret:
        credential['dbInstanceIdentifier'] = secret['dbInstanceIdentifier']   
       
    if "db_name" in secret:
        credential['dbname'] = secret['db_name']
        
    if "dbname" in secret:
        credential['dbname'] = secret['dbname']
    
    return credential
    
credential = getCredentials(secretNameDwh) 

url = credential['url']
us = credential['username']
ps = credential['password']

etl_date = date.today()
end_date = '99991231'

ar_tvr_smy_data = spark.read.format("jdbc").option("url",url).option("user",us).option("password",ps).option("dbtable","(select * from hoand.ar_tvr_smy)").load()
ar_tvr_smy_data.createOrReplaceTempView("ar_tvr_smy")

ar_data = spark.read.format("jdbc").option("url",url).option("user",us).option("password",ps).option("dbtable","(select * from hoand.ar)").load()
ar_data.createOrReplaceTempView("ar")

pd_data = spark.read.format("jdbc").option("url",url).option("user",us).option("password",ps).option("dbtable","(select * from hoand.pd)").load()
pd_data.createOrReplaceTempView("pd")

cst_data = spark.read.format("jdbc").option("url",url).option("user",us).option("password",ps).option("dbtable","(select * from hoand.cst)").load()
cst_data.createOrReplaceTempView("cst")

ou_data = spark.read.format("jdbc").option("url",url).option("user",us).option("password",ps).option("dbtable","(select * from hoand.ou)").load()
ou_data.createOrReplaceTempView("ou")

ac_ar_dim_data = spark.read.format("jdbc").option("url",url).option("user",us).option("password",ps).option("dbtable","(select * from hoand.ac_ar_dim)").load()
ac_ar_dim_data.createOrReplaceTempView("ac_ar_dim")

arx_pd_rltnp_data = spark.read.format("jdbc").option("url",url).option("user",us).option("password",ps).option("dbtable","(select * from hoand.arx_pd_rltnp)").load()
arx_pd_rltnp_data.createOrReplaceTempView("arx_pd_rltnp")

arx_ip_rltnp_data = spark.read.format("jdbc").option("url",url).option("user",us).option("password",ps).option("dbtable","(select * from hoand.arx_ip_rltnp)").load()
arx_ip_rltnp_data.createOrReplaceTempView("arx_ip_rltnp")

pd_dim_data = spark.read.format("jdbc").option("url",url).option("user",us).option("password",ps).option("dbtable","(select * from hoand.pd_dim)").load()
pd_dim_data.createOrReplaceTempView("pd_dim")

cst_dim_data = spark.read.format("jdbc").option("url",url).option("user",us).option("password",ps).option("dbtable","(select * from hoand.cst_dim)").load()
cst_dim_data.createOrReplaceTempView("cst_dim")

ou_dim_data = spark.read.format("jdbc").option("url",url).option("user",us).option("password",ps).option("dbtable","(select * from hoand.ou_dim)").load()
ou_dim_data.createOrReplaceTempView("ou_dim")

ccy_dim_data = spark.read.format("jdbc").option("url",url).option("user",us).option("password",ps).option("dbtable","(select * from hoand.ccy_dim)").load()
ccy_dim_data.createOrReplaceTempView("ccy_dim")


sql = f"""
select
    ac_ar_dim.ac_ar_dim_id as ACG_AR_DIM_ID,
    cst_dim.cst_dim_id as CST_DIM_ID,
    ccy_dim.ccy_dim_id as CCY_DIM_ID,
    pd_dim.pd_dim_id as PD_DIM_ID,
    ou_dim.ou_dim_id as OU_DIM_ID,
    cast(smy.cls_bal_fcy as bigint) as CLS_BAL_FCY,
    cast(smy.cls_bal_lcy as bigint) as CLS_BAL_LCY,
    cast(replace('{etl_date}','-','') as int) as RPT_DT_DIM_ID
from ar_tvr_smy smy inner join ar on smy.ar_id = ar.ar_id
                    left join ac_ar_dim on ar.UNQ_ID_IN_SRC_STM = ac_ar_dim.Ac_NBR and ac_ar_dim.eff_dt <='{etl_date}' and '{etl_date}' < ac_ar_dim.end_dt
                    left join arx_pd_rltnp ar_x_pd on ar.ar_id = ar_x_pd.ar_id and ar_x_pd.eff_dt <='{etl_date}' and '{etl_date}' < ar_x_pd.end_dt 
                        and ar_x_pd.ARX_PD_RLTNP_TP_ID = sha2('CL|AR_X_PD_TP|MASTER_PRODUCT' , 256)
                    left join pd on ar_x_pd.pd_id = pd.pd_id
                    left join pd_dim on pd.UNQ_ID_IN_SRC_STM = pd_dim.pd_code and pd_dim.eff_dt <='{etl_date}' and '{etl_date}' < pd_dim.end_dt
                    left join arx_ip_rltnp ar_x_ip on ar.ar_id = ar_x_ip.ar_id and ar_x_ip.eff_dt <='{etl_date}' and '{etl_date}' < ar_x_ip.end_dt
                        and (ar_x_ip.arx_ip_rltnp_tp_id = sha2('CL|AR_X_IP_TP|AR_X_CST' , 256) or ar_x_ip.arx_ip_rltnp_tp_id = sha2('CL|AR_X_IP_TP|AR_X_OU' , 256))
                    left join cst on ar_x_ip.ip_id = cst.cst_id
                    left join cst_dim on cst.UNQ_ID_IN_SRC_STM = cst_dim.cst_code and cst_dim.eff_dt <='{etl_date}' and '{etl_date}' < cst_dim.end_dt
                    left join ou on ar_x_ip.ip_id = ou.ou_id
                    left join ou_dim on ou.ou_code = ou_dim.ou_code and ou_dim.eff_dt <='{etl_date}' and '{etl_date}' < ou_dim.end_dt
                    left join ccy_dim on smy.ccy_id = ccy_dim.ccy_id and ccy_dim.eff_dt <='{etl_date}' and '{etl_date}' < ccy_dim.end_dt"""
                    
ar_analyis_fct_df = spark.sql(sql)
ar_analyis_fct_data = DynamicFrame.fromDF(ar_analyis_fct_df, glueContext,'ar_analyis_fct_data')

pre_query = f"delete from hoand.ar_analyis_fct where RPT_DT_DIM_ID = cast(replace('{etl_date}','-','') as int) and 1=1"

write_data= glueContext.write_dynamic_frame.from_jdbc_conf(
frame = ar_analyis_fct_data, catalog_connection ="ducph_redshift_connection", redshift_tmp_dir=args["TempDir"], transformation_ctx="write_data",
connection_options = {"preactions":pre_query,"dbtable": "hoand.ar_analyis_fct", "database": "dev"})



job.commit()