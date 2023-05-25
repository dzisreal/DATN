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

ou_data = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable","(select * from hoand.ou)").load()
ou_data.createOrReplaceTempView("ou")

sql_ext = f"""
(select  * 
  from hoand.ou_dim a 
  where a.EFF_DT <= '{etl_date}' and '{end_date}' <= a.END_DT )"""

ou_dim_data = spark.read.format("jdbc").option("url","jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev").option("user",us).option("password",ps).option("dbtable",sql_ext).load()
ou_dim_data.createOrReplaceTempView("ou_dim")

sql =  f""" with tmp as (
select 
    a.ou_id as ou_ID,
    a.ou_code as ou_CODE,
    a.SRC_STM_ID as SRC_STM_ID,
    to_date('{etl_date}','yyyy-MM-dd') as EFF_DT,
    to_date('99991231','yyyyMMdd') as END_DT
from ou a)
SELECT tmp.*
     , case when t.ou_ID is null then 'I' else 'U' end REC_IND 
FROM tmp 
LEFT JOIN ou_dim t ON tmp.ou_ID = t.ou_ID
where nvl(sha2(concat_ws('*',nvl(tmp.ou_ID,'$'),nvl(tmp.ou_CODE,'$'),nvl(tmp.SRC_STM_ID,'$')),256),'$') <> nvl(sha2(concat_ws('*',nvl(t.ou_ID,'$'),nvl(t.ou_CODE,'$'),nvl(t.SRC_STM_ID,'$')),256),'$')
"""

twt_ou_dim_df = spark.sql(sql)
twt_ou_dim_data = DynamicFrame.fromDF(twt_ou_dim_df,glueContext,'twt_ou_dim_data')

pre_query = 'truncate table hoand.twt_ou_dim'

post_query = f"""
begin transaction;

update hoand.ou_dim
set END_DT = '{etl_date}'
where '{etl_date}' between hoand.ou_dim.eff_dt and hoand.ou_dim.END_DT
and exists (
		select 1
		from hoand.twt_ou_dim twt
		where hoand.ou_dim.ou_ID = twt.ou_ID
		 and twt.REC_IND = 'U'
		);

insert into hoand.ou_dim
        (ou_ID,
        ou_CODE,
        SRC_STM_ID,
        EFF_DT,
        END_DT
)
    (
    select
        ou_ID,
        ou_CODE,
        SRC_STM_ID,
        EFF_DT,
        END_DT

    from
        hoand.twt_ou_dim
    ) ;

end transaction;"""

write_data= glueContext.write_dynamic_frame.from_jdbc_conf(
frame = twt_ou_dim_data, catalog_connection ="ducph_redshift_connection", redshift_tmp_dir=args["TempDir"], transformation_ctx="write_data",
connection_options = {"preactions":pre_query,"dbtable": "hoand.twt_ou_dim","postactions":post_query, "database": "dev"})


job.commit()