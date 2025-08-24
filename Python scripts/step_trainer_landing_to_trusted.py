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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1755959563097 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://ct-s3-bucket-1/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1755959563097")

# Script generated for node Customer Curated
CustomerCurated_node1755959641293 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://ct-s3-bucket-1/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1755959641293")

# Script generated for node SQL Query
SqlQuery0 = '''
select sl.sensorreadingtime
, sl.serialnumber
, sl.distancefromobject
FROM customer_curated cc
    JOIN step_trainer_landing sl
    ON cc.serialnumber = sl.serialnumber;

'''
SQLQuery_node1755959689500 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":StepTrainerLanding_node1755959563097, "customer_curated":CustomerCurated_node1755959641293}, transformation_ctx = "SQLQuery_node1755959689500")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1755959689500, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1755959536301", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1755959732287 = glueContext.getSink(path="s3://ct-s3-bucket-1/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1755959732287")
StepTrainerTrusted_node1755959732287.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1755959732287.setFormat("json")
StepTrainerTrusted_node1755959732287.writeFrame(SQLQuery_node1755959689500)
job.commit()