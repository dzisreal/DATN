import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime,date
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

etl_date = date.today()
end_date = '99991231'

us = "awsuser"
ps = "Ducph4010"

cst_data = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable","(select * from hoand.cst)").load()
cst_data.createOrReplaceTempView("cst")

ip_data = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable","(select * from hoand.ip)").load()
ip_data.createOrReplaceTempView("ip")

ipx_ip_rltnp_data = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable","(select * from hoand.ipx_ip_rltnp)").load()
ipx_ip_rltnp_data.createOrReplaceTempView("ipx_ip_rltnp")

ou_data = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable","(select * from hoand.ou)").load()
ou_data.createOrReplaceTempView("ou")

sql_ext = f"""
(select  * 
  from hoand.cst_dim a 
  where a.EFF_DT <= '{etl_date}' and '{end_date}' <= a.END_DT )"""

cst_dim_data = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable",sql_ext).load()
cst_dim_data.createOrReplaceTempView("cst_dim")

sql =  f""" with tmp as (
select 
    cst.cst_id as CST_ID,
    cst.UNQ_ID_IN_SRC_STM as CST_CODE,
    ip.IP_NM as CST_NM,
    cst.CST_TP_ID as CST_TP,
    cst.cst_lcs_tp_id as  CST_LC_ST_TP,
    ou.ou_code as PRIM_OU_CODE,
    to_date(cst.EFF_CST_DT,'yyyy-MM-dd') as EFF_CST_DATE,
    to_date(cst.END_CST_DT,'yyyy-MM-dd') as END_CST_DATE,
    to_date('{etl_date}','yyyy-MM-dd') as EFF_DT,
    to_date('99991231','yyyyMMdd') as END_DT
from cst left join ip on cst.cst_id = ip.ip_id
         left join ipx_ip_rltnp cst_x_ou on ip.ip_id = cst_x_ou.sbj_ip_id and cst_x_ou.ipx_ip_rltnp_tp_id = sha2('CL|IP_X_IP_TP|IP_X_OU' , 256) and cst_x_ou.eff_dt <= '{etl_date}' and '{etl_date}' < cst_x_ou.end_dt
         left join ou on ou.ou_id = cst_x_ou.OBJ_IP_ID
        )
SELECT tmp.*
     , case when t.CST_ID is null then 'I' else 'U' end REC_IND 
FROM tmp 
LEFT JOIN cst_dim t ON tmp.CST_ID = t.CST_ID
where nvl(sha2(concat_ws('*',nvl(tmp.CST_ID,'$'),nvl(tmp.CST_CODE,'$'),nvl(tmp.CST_NM,'$'),nvl(tmp.CST_TP,'$'),nvl(tmp.CST_LC_ST_TP,'$'),nvl(tmp.PRIM_OU_CODE,'$')),256),'$') <> nvl(sha2(concat_ws('*',nvl(t.CST_ID,'$'),nvl(t.CST_CODE,'$'),nvl(t.CST_NM,'$'),nvl(t.CST_TP,'$'),nvl(t.CST_LC_ST_TP,'$'),nvl(t.PRIM_OU_CODE,'$')),256),'$')
"""

twt_cst_dim_df = spark.sql(sql)
twt_cst_dim_data = DynamicFrame.fromDF(twt_cst_dim_df,glueContext,'twt_cst_dim_data')

pre_query = 'truncate table hoand.twt_cst_dim'

post_query = f"""
begin transaction;

update hoand.cst_dim
set END_DT = '{etl_date}'
where '{etl_date}' between hoand.cst_dim.eff_dt and hoand.cst_dim.END_DT
and exists (
		select 1
		from hoand.twt_cst_dim twt
		where hoand.cst_dim.CST_ID = twt.CST_ID
		 and twt.REC_IND = 'U'
		);

insert into hoand.cst_dim
        (CST_ID,
        CST_CODE,
        CST_NM,
        CST_TP,
        CST_LC_ST_TP,
        PRIM_OU_CODE,
        EFF_CST_DATE,
        END_CST_DATE,
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
        EFF_CST_DATE,
        END_CST_DATE,
        EFF_DT,
        END_DT
    from
        hoand.twt_cst_dim
    ) ;

end transaction;"""

write_data= glueContext.write_dynamic_frame.from_jdbc_conf(
frame = twt_cst_dim_data, catalog_connection ="ducph_redshift_connection", redshift_tmp_dir=args["TempDir"], transformation_ctx="write_data",
connection_options = {"preactions":pre_query,"dbtable": "hoand.twt_cst_dim","postactions":post_query, "database": "dev"})



job.commit()