import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime,date, timedelta
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


#pDate = str(date.today())
pDate = '2022-09-30'
pTime = str(datetime.now())
pMaxDate = '9999-12-31'


us = "awsuser"
ps = "Ducph4010"

# Script generated for node S3 bucket

sql_ext = """
(select  * 
  from dwh.OU_HDK 
 )
""".format(pDate=pDate)
DF_OU = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable",sql_ext).load()
DF_OU.createOrReplaceTempView("OU") 



sql_ext = """
(select  * 
  from dwh.ou_dim_hdk )
""".format(pDate=pDate)
TWT_OU_DIM_HDK = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable",sql_ext).load()
TWT_OU_DIM_HDK.createOrReplaceTempView("OU_DIM_HDK_VIEW")



df = spark.sql("""
    with tmp as (SELECT 
    OU.OU_ID,
    OU.ORG_CODE as OU_CODE,
    OU.SRC_STM_ID as SRC_STM_ID,
    to_date('2022-09-30','yyyy-MM-dd') as EFF_DT,
    to_date('9999-12-31','yyyy-MM-dd') as END_DT

from OU)


SELECT tmp.*
     , case when t.OU_ID is null then 'I' else 'U' end REC_IND 
FROM  tmp     
LEFT JOIN OU_DIM_HDK_VIEW  t 
ON  tmp.OU_ID = t.OU_ID 
where COALESCE(sha2(concat_ws('*',COALESCE(tmp.OU_ID,'$'),COALESCE(tmp.OU_CODE,'$'),COALESCE(tmp.SRC_STM_ID,'$'),COALESCE(tmp.EFF_DT,'$'),COALESCE(tmp.END_DT,'$')),256),'$') <> COALESCE(sha2(concat_ws('*',COALESCE(t.OU_ID,'$'),COALESCE(t.OU_CODE,'$'),COALESCE(t.SRC_STM_ID,'$'), COALESCE(t.EFF_DT,'$'),COALESCE(t.END_DT,'$')),256),'$')
""".format(pDate=pDate,pTime=pTime,pMaxDate=pMaxDate))


TWT_OU_DIM_HDK_DyF= DynamicFrame.fromDF(df, glueContext, "TWT_OU_DIM_HDK_DyF")

pre_query = "TRUNCATE TABLE DWH.twt_OU_DIM_HDK"

post_query = """begin transaction; UPDATE dwh.OU_DIM_HDK set END_DT = '{pDate}' where '{pDate}' between dwh.OU_DIM_HDK.EFF_DT and dwh.OU_DIM_HDK.END_DT and exists (select 1 from dwh.twt_OU_DIM_HDK twt where dwh.OU_DIM_HDK.OU_ID = twt.OU_ID and twt.REC_IND = 'U');		
Insert into dwh.OU_DIM_HDK (
        OU_ID,
        OU_CODE,
        SRC_STM_ID,
        EFF_DT,
        END_DT
       )
    (
    select
          OU_ID,
        OU_CODE,
        SRC_STM_ID,
        EFF_DT,
        END_DT
    from
        dwh.twt_OU_DIM_HDK 
    ) ;
    end transaction;""".format(pDate=pDate, pMaxDate = pMaxDate)
    
##Write Data
write_data= glueContext.write_dynamic_frame.from_jdbc_conf(
frame = TWT_OU_DIM_HDK_DyF, catalog_connection ="ducph_redshift_connection", redshift_tmp_dir=args["TempDir"], transformation_ctx="write_data",
connection_options = {"preactions":pre_query,"dbtable": "DWH.twt_OU_DIM_HDK","postactions":post_query,"database": "dev"}) 


job.commit()