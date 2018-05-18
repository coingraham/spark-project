import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
 
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

writer = glueContext.create_dynamic_frame.from_catalog(
    database = "legislators", 
    table_name = "events_json").write(
        connection_type = "s3", 
        connection_options = {"path": "s3://aws-glue-test-legislator-data/local-copy/events"}, 
        format = "parquet")

job.commit()