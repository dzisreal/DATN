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

ccy_data = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable","(select * from hoand.ccy)").load()
ccy_data.createOrReplaceTempView("ccy")

sql_ext = f"""
(select  * 
  from hoand.ccy_dim a 
  where a.EFF_DT <= '{etl_date}' and '{end_date}' <= a.END_DT )"""

ccy_dim_data = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable",sql_ext).load()
ccy_dim_data.createOrReplaceTempView("ccy_dim")

sql =  f""" with tmp as (
select 
    a.ccy_id as CCY_ID,
    a.ccy_code as CCY_CODE,
    a.SRC_STM_ID as SRC_STM_ID,
    to_date('{etl_date}','yyyy-MM-dd') as EFF_DT,
    to_date('99991231','yyyyMMdd') as END_DT
from ccy a)
SELECT tmp.*
     , case when t.CCY_ID is null then 'I' else 'U' end REC_IND 
FROM tmp 
LEFT JOIN ccy_dim t ON tmp.CCY_ID = t.CCY_ID
where nvl(sha2(concat_ws('*',nvl(tmp.CCY_ID,'$'),nvl(tmp.CCY_CODE,'$'),nvl(tmp.SRC_STM_ID,'$')),256),'$') <> nvl(sha2(concat_ws('*',nvl(t.CCY_ID,'$'),nvl(t.CCY_CODE,'$'),nvl(t.SRC_STM_ID,'$')),256),'$')
"""

twt_ccy_dim_df = spark.sql(sql)
twt_ccy_dim_data = DynamicFrame.fromDF(twt_ccy_dim_df,glueContext,'twt_ccy_dim_data')

pre_query = 'truncate table hoand.twt_ccy_dim'

post_query = f"""
begin transaction;

update hoand.ccy_dim
set END_DT = '{etl_date}'
where '{etl_date}' between hoand.ccy_dim.eff_dt and hoand.ccy_dim.END_DT
and exists (
		select 1
		from hoand.twt_ccy_dim twt
		where hoand.ccy_dim.ccy_ID = twt.ccy_ID
		 and twt.REC_IND = 'U'
		);

insert into hoand.ccy_dim
        (ccy_ID,
        ccy_CODE,
        SRC_STM_ID,
        EFF_DT,
        END_DT
)
    (
    select
        ccy_ID,
        ccy_CODE,
        SRC_STM_ID,
        EFF_DT,
        END_DT

    from
        hoand.twt_ccy_dim
    ) ;

end transaction;"""

write_data= glueContext.write_dynamic_frame.from_jdbc_conf(
frame = twt_ccy_dim_data, catalog_connection ="ducph_redshift_connection", redshift_tmp_dir=args["TempDir"], transformation_ctx="write_data",
connection_options = {"preactions":pre_query,"dbtable": "hoand.twt_ccy_dim","postactions":post_query, "database": "dev"})


job.commit()