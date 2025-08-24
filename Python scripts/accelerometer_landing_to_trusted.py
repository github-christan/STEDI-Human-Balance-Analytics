import sys
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

DB = "stedi"
TARGET_TABLE = "accelerometer_trusted"
TARGET_S3 = "s3://ct-s3-bucket-1/accelerometer/trusted/"
LANDING_S3 = "s3://ct-s3-bucket-1/accelerometer/landing/"

DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# ---- Sources ----
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database=DB, table_name="customer_trusted", transformation_ctx="CustomerTrusted_node"
)

# Read landing directly from S3 (avoid any bad catalog metadata)
accelerometer_landing = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [LANDING_S3]},
    format="json",
    transformation_ctx="AccelerometerLanding_node_s3"
)

# ---- Transform (keep reserved names) ----
SqlQuery0 = """
SELECT 
    b.timeStamp AS `timestamp`,
    b.`user`    AS `user`,
    b.x,
    b.y,
    b.z
FROM customer_trusted a
INNER JOIN accelerometer_landing b
    ON a.email = b.`user`
"""
result_dyf = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"customer_trusted": customer_trusted, "accelerometer_landing": accelerometer_landing},
    transformation_ctx="SQLQuery_node"
)

# ---- (Optional) Data Quality ----
EvaluateDataQuality().process_rows(
    frame=result_dyf,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# ---- Catalog / Write (single writer path) ----
# Ensure DB exists; drop stale table metadata if any
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
spark.sql(f"DROP TABLE IF EXISTS {DB}.{TARGET_TABLE}")

# Purge existing files under the target prefix to avoid duplicates
glueContext.purge_s3_path(TARGET_S3, options={"retentionPeriod": 0})

# Create/Update the Glue Catalog table and write files in one step
sink = glueContext.getSink(
    path=TARGET_S3,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],  # add partitions later if needed
    enableUpdateCatalog=True,
    transformation_ctx="CatalogSink_accel_trusted",
)
sink.setCatalogInfo(catalogDatabase=DB, catalogTableName=TARGET_TABLE)
sink.setFormat("json")
sink.writeFrame(result_dyf)

job.commit()
