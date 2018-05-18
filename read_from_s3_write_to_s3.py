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

persons = glueContext.create_dynamic_frame.from_catalog(database="legislators", table_name="persons_json")

print "Count: ", persons.count()
persons.printSchema()

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path":"s3://aws-glue-test-legislator-data/output-dir/test/"}, format = "parquet", format_options = <format_options>, transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: [frame = <frame>]
write_response = glueContext.write_dynamic_frame.from_options(frame = persons, connection_type = "s3", connection_options = {"path":"s3://aws-glue-test-legislator-data/output-dir/test/"}, format = "parquet")
 
job.commit()