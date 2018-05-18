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

persons = glueContext.create_dynamic_frame.from_catalog(database = "legislators", table_name = "persons_json")

persons_spig = Spigot.apply(frame = persons, path = "s3://aws-glue-test-legislator-data/output-dir/persons-spigot", options = {'prob':.20})

memberships = glueContext.create_dynamic_frame.from_catalog(database = "legislators", table_name = "memberships_json")

memberships_spig = Spigot.apply(frame = memberships, path = "s3://aws-glue-test-legislator-data/output-dir/memberships-spigot", options = {'prob':.20})

person_member = Join.apply(frame1 = persons_spig, frame2 = memberships_spig, keys1 = ["id"], keys2 = ["person_id"])

person_member_spig = Spigot.apply(frame = person_member, path = "s3://aws-glue-test-legislator-data/output-dir/person-member-spigot", options = {'prob':.20})

person_member_spig.drop_fields(["person_id"])
 
job.commit()