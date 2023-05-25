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

ri_data = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable","(select * from hoand.ri)").load()
ri_data.createOrReplaceTempView("ri")

sql_ext = f"""
(select  * 
  from hoand.RSC_DIM a 
  where a.EFF_DT <= '{etl_date}' and '{end_date}' <= a.END_DT )"""

rsc_dim_data = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable",sql_ext).load()
rsc_dim_data.createOrReplaceTempView("rsc_dim")

sql = f""" with tmp as (
select 
    a.ri_id  as RSC_ID,
    a.UNQ_ID_IN_SRC_STM as RSC_CODE,
    to_date('{etl_date}','yyyy-MM-dd') as EFF_DT,
    to_date('{end_date}','yyyyMMdd') as END_DT
from ri a)
SELECT tmp.*
     , case when t.RSC_ID is null then 'I' else 'U' end REC_IND 
FROM tmp 
LEFT JOIN rsc_dim t ON tmp.RSC_ID = t.RSC_ID
where nvl(sha2(concat_ws('*',nvl(tmp.RSC_ID,'$'),nvl(tmp.RSC_CODE,'$')),256),'$') <> nvl(sha2(concat_ws('*',nvl(t.RSC_ID,'$'),nvl(t.RSC_CODE,'$')),256),'$')
"""
twt_rsc_dim_df = spark.sql(sql)
twt_rsc_dim_data = DynamicFrame.fromDF(twt_rsc_dim_df,glueContext,'twt_rsc_dim_data')

pre_query = 'truncate table hoand.twt_rsc_dim'

post_query = f"""
begin transaction;

update hoand.rsc_dim
set END_DT = '{etl_date}'
where '{etl_date}' between hoand.rsc_dim.eff_dt and hoand.rsc_dim.END_DT
and exists (
		select 1
		from hoand.twt_rsc_dim twt
		where hoand.rsc_dim.RSC_ID = twt.RSC_ID
		 and twt.REC_IND = 'U'
		);

insert into hoand.rsc_dim
        (RSC_ID,
        RSC_CODE,
        EFF_DT,
        END_DT
)
    (
    select
        RSC_ID,
        RSC_CODE,
        EFF_DT,
        END_DT
    from
        hoand.twt_rsc_dim
    ) ;

end transaction;"""

write_data= glueContext.write_dynamic_frame.from_jdbc_conf(
frame = twt_rsc_dim_data, catalog_connection ="ducph_redshift_connection", redshift_tmp_dir=args["TempDir"], transformation_ctx="write_data",
connection_options = {"preactions":pre_query,"dbtable": "hoand.twt_rsc_dim","postactions":post_query, "database": "dev"})

job.commit()