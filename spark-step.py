from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("SparkEMR")\
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sql_context = SQLContext(sc)

    persons = sql_context.sql("SELECT * FROM legislators.persons_json")

    persons.write.parquet("s3://aws-glue-test-legislator-data/emr-copy/persons", mode="overwrite")

    sc.stop()