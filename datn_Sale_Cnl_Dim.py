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
  from dwh.ou_hdk
) 
""".format(pDate=pDate)
df_OU= spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable",sql_ext).load()
df_OU.createOrReplaceTempView("OU") 

sql_ext = """
(select  * 
  from dwh.IP_hdk
) 
""".format(pDate=pDate)
df_IP = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable",sql_ext).load()
df_IP.createOrReplaceTempView("IP") 


sql_ext = """
(select  * 
  from dwh.CST_HDK 
 )
""".format(pDate=pDate)
CST_HDK = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable",sql_ext).load()
CST_HDK.createOrReplaceTempView("CST") 

sql_ext = """
(select  * 
  from dwh.ip_x_cl_rltnp_hdk )
""".format(pDate=pDate)
AR_X_CL_RLTNP = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable",sql_ext).load()
AR_X_CL_RLTNP.createOrReplaceTempView("AR_X_CL_RLTNP")


sql_ext = """
(select  * 
  from dwh.CST_DIM_HDK
  where EFF_DT <= '{pDate}' and '{pDate}' < END_DT ) as t1
""".format(pDate=pDate)
CST_DIM_HDK = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable",sql_ext).load()
CST_DIM_HDK.createOrReplaceTempView("CST_DIM_HDK_VIEW")



df = spark.sql("""
    with tmp as (select CST.CST_ID,
    CST.UNQ_ID_IN_SRC_STM as CST_CODE,
    IP.IP_NM as CST_NM ,
    CST.CST_TP as CST_TP,
    CST.CST_lcs_TP_ID as CST_LC_ST_TP,
    ou.ou_id  as PRIM_OU_CODE
    , to_date('2022-09-30','yyyy-MM-dd') as EFF_CST_DT
    , to_date('9999-12-31','yyyy-MM-dd') as END_CST_DT
    , to_date('2022-09-30','yyyy-MM-dd') as EFF_DT
    , to_date('9999-12-31','yyyy-MM-dd') as END_DT
from CST
left join  IP 
ON CST.CST_ID = IP.IP_ID
left join AR_X_CL_RLTNP as CST_X_OU
ON ip.IP_ID = cst_x_ou.IP_ID AND CST_X_OU.IPX_CL_RLTNP_TP_ID = sha2('CL' || '|' || 'IP_X_CL_TP' || '|' || 'IP_X_LO_TP', 256) AND cst_x_ou.EFF_DT <= '2022-09-30' and '2022-09-30' < cst_x_ou.END_DT
left join OU
on cst_x_ou.CL_ID = ou.OU_ID
)


SELECT tmp.*
     , case when t.CST_ID is null then 'I' else 'U' end REC_IND 
FROM  tmp     
LEFT JOIN CST_DIM_HDK_VIEW  t 
ON  tmp.CST_ID = t.CST_ID 
where 
COALESCE(sha2(concat_ws('*',COALESCE(tmp.CST_ID,'$'),
COALESCE(tmp.CST_CODE,'$'),
COALESCE(tmp.CST_NM,'$'),
COALESCE(tmp.CST_TP,'$'),
COALESCE(tmp.CST_LC_ST_TP,'$'),
COALESCE(tmp.PRIM_OU_CODE,'$'),
COALESCE(tmp.EFF_CST_DT,'$'),
COALESCE(tmp.END_CST_DT,'$'),
COALESCE(tmp.EFF_DT,'$'),
COALESCE(tmp.END_DT,'$')),256),'$')
<> 
COALESCE(sha2(concat_ws('*',COALESCE(t.CST_ID,'$'),
COALESCE(t.CST_CODE,'$'),
COALESCE(t.CST_NM,'$'),
COALESCE(t.CST_TP,'$'),
COALESCE(t.CST_LC_ST_TP,'$'),
COALESCE(t.PRIM_OU_CODE,'$'),
COALESCE(t.EFF_CST_DT,'$'),
COALESCE(t.END_CST_DT,'$'),
COALESCE(t.EFF_DT,'$'),
COALESCE(t.END_DT,'$')),256),'$')
""".format(pDate=pDate,pTime=pTime,pMaxDate=pMaxDate))


TWT_CST_DIM_HDK_DyF= DynamicFrame.fromDF(df, glueContext, "TWT_CST_DIM_HDK_DyF")

pre_query = "TRUNCATE TABLE DWH.TWT_CST_DIM_HDK"

post_query = """begin transaction; UPDATE dwh.CST_DIM_HDK set END_DT = '{pDate}' where '{pDate}' between dwh.CST_DIM_HDK.EFF_DT and dwh.CST_DIM_HDK.END_DT and exists (select 1 from dwh.TWT_CST_DIM_HDK twt where dwh.CST_DIM_HDK.CST_ID = twt.CST_ID and twt.REC_IND = 'U');		
Insert into dwh.CST_DIM_HDK (
        CST_ID,
        CST_CODE,
        CST_NM,
        CST_TP,
        CST_LC_ST_TP,
        PRIM_OU_CODE,
        EFF_CST_DT,
        END_CST_DT,
        EFF_DT,
        END_DT
       )
    (
    select
         CST_ID,
        CST_CODE,
        CST_NM,
        CST_TP,
        CST_LC_ST_TP,
        PRIM_OU_CODE,
        EFF_CST_DT,
        END_CST_DT,
        EFF_DT,
        END_DT
    from
        dwh.TWT_CST_DIM_HDK 
    ) ;
    end transaction;""".format(pDate=pDate, pMaxDate = pMaxDate)
    
##Write Data
write_data= glueContext.write_dynamic_frame.from_jdbc_conf(
frame = TWT_CST_DIM_HDK_DyF, catalog_connection ="ducph_redshift_connection", redshift_tmp_dir=args["TempDir"], transformation_ctx="write_data",
connection_options = {"preactions":pre_query,"dbtable": "DWH.TWT_CST_DIM_HDK","postactions":post_query,"database": "dev"}) 


job.commit()