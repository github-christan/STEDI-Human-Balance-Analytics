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

# Script generated for node Customer Landing
CustomerLanding_node1754450378184 = glueContext.create_dynamic_frame.from_catalog(database="customer_db", table_name="landing", transformation_ctx="CustomerLanding_node1754450378184")

# Script generated for node SQL Query
SqlQuery125 = '''
select * from customerLanding
where shareWithResearchAsOfDate is not null
'''
SQLQuery_node1754613447752 = sparkSqlQuery(glueContext, query = SqlQuery125, mapping = {"customerLanding":CustomerLanding_node1754450378184}, transformation_ctx = "SQLQuery_node1754613447752")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1754613447752, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1754448890964", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1754450165521 = glueContext.getSink(path="s3://ct-s3-bucket-1/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1754450165521")
CustomerTrusted_node1754450165521.setCatalogInfo(catalogDatabase="customer_db",catalogTableName="customer_trusted")
CustomerTrusted_node1754450165521.setFormat("json")
CustomerTrusted_node1754450165521.writeFrame(SQLQuery_node1754613447752)
job.commit()