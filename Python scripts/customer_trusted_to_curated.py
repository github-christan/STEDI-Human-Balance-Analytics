import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1755352375596 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="AmazonS3_node1755352375596")

# Script generated for node Amazon S3
AmazonS3_node1755352268603 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AmazonS3_node1755352268603")

# Script generated for node Join
Join_node1755352408024 = Join.apply(frame1=AmazonS3_node1755352375596, frame2=AmazonS3_node1755352268603, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1755352408024")

# Script generated for node Drop Fields and Duplicates
SqlQuery193 = '''
select distinct customerName
, email
, phone
, birthday
, serialNumber
, registrationDate
, lastUpdateDate
, shareWithResearchAsOfDate
, shareWithPublicAsOfDate
, shareWithFriendsAsOfDate
from myDataSource
'''
DropFieldsandDuplicates_node1755353154133 = sparkSqlQuery(glueContext, query = SqlQuery193, mapping = {"myDataSource":Join_node1755352408024}, transformation_ctx = "DropFieldsandDuplicates_node1755353154133")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFieldsandDuplicates_node1755353154133, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1755351721553", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1755352450043 = glueContext.getSink(path="s3://ct-s3-bucket-1/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1755352450043")
AmazonS3_node1755352450043.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1755352450043.setFormat("json")
AmazonS3_node1755352450043.writeFrame(DropFieldsandDuplicates_node1755353154133)
job.commit()