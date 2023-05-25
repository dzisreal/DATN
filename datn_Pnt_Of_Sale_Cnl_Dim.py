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


sql_ext = """
(select * 
  from dwh.CCY_HDK) 
"""
CCY_HDKt = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable",sql_ext).load()
CCY_HDKt.createOrReplaceTempView("CCY") 

sql_ext = """
(select  * 
  from dwh.CCY_DIM_hdk a
  where a.EFF_DT <= '{pDate}' and '{pMaxDate}' < a.END_DT ) as t1
""".format(pDate=pDate, pMaxDate=pMaxDate)
df_CCY_DIM_ntdat = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable",sql_ext).load()
df_CCY_DIM_ntdat.createOrReplaceTempView("CCY_DIM_HDK_VIEW")

sql = """
with tmp as (
select
    ccy.CCY_ID as CCY_ID
    , ccy.CCY_CODE as CCY_CODE
    , ccy.SRC_STM_ID as SRC_STM_ID
    , to_date('{pDate}','yyyy-MM-dd') as EFF_DT
    , to_date('{pMaxDate}','yyyy-MM-dd') as END_DT
from CCY ccy
)

SELECT tmp.*
     , case when t.CCY_ID is null then 'I' else 'U' end REC_IND 
FROM  tmp     
LEFT JOIN CCY_DIM_HDK_VIEW t 
ON  tmp.CCY_ID = t.CCY_ID 
where COALESCE(sha2(concat_ws('*',COALESCE(tmp.CCY_ID,'$'),
COALESCE(tmp.CCY_CODE,'$'),
COALESCE(tmp.SRC_STM_ID,'$'),
COALESCE(tmp.EFF_DT,'$'),
COALESCE(tmp.END_DT,'$')),256),'$') 
<> 
COALESCE(sha2(concat_ws('*',COALESCE(t.CCY_ID,'$'),
COALESCE(t.CCY_CODE,'$'),
COALESCE(t.SRC_STM_ID,'$'),
COALESCE(t.EFF_DT,'$'),
COALESCE(t.END_DT,'$')),256),'$')
""".format(pDate=pDate,pTime=pTime,pMaxDate=pMaxDate)

TWT_CCY_DIM_HDK_DF = spark.sql(sql)
TWT_CCY_DIM_HDK_DyF= DynamicFrame.fromDF(TWT_CCY_DIM_HDK_DF, glueContext, "TWT_CCY_DIM_HDK_DyF")

pre_query = "TRUNCATE TABLE DWH.TWT_CCY_DIM_HDK"

post_query = """begin transaction; UPDATE dwh.CCY_DIM_HDK set END_DT = '{pDate}' where '{pDate}' between dwh.CCY_DIM_HDK.EFF_DT and dwh.CCY_DIM_HDK.END_DT and exists (select 1 from dwh.TWT_CCY_DIM_HDK twt where dwh.CCY_DIM_HDK.CCY_ID = twt.CCY_ID and twt.REC_IND = 'U');		
Insert into dwh.CCY_DIM_HDK (
        CCY_ID,
        CCY_CODE,
        SRC_STM_ID,
        EFF_DT,
        END_DT
       )
    (
    select
        CCY_ID,
        CCY_CODE,
        SRC_STM_ID,
        EFF_DT,
        END_DT
    from
        dwh.TWT_CCY_DIM_HDK
    ) ;
    end transaction;""".format(pDate=pDate, pMaxDate = pMaxDate)
    
##Write Data
write_data= glueContext.write_dynamic_frame.from_jdbc_conf(
frame = TWT_CCY_DIM_HDK_DyF, catalog_connection ="ducph_redshift_connection", redshift_tmp_dir=args["TempDir"], transformation_ctx="write_data",
connection_options = {"preactions":pre_query,"dbtable": "DWH.TWT_CCY_DIM_HDK","postactions":post_query, "database": "dev"})  
	
job.commit()