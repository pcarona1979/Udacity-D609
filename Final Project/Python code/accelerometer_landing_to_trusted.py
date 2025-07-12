import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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
AmazonS3_node1752274256074 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://datalake71125/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1752274256074")

# Script generated for node Customer_trusted
Customer_trusted_node1752274558374 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://datalake71125/customer/trusted/"], "recurse": True}, transformation_ctx="Customer_trusted_node1752274558374")

# Script generated for node Join
Join_node1752274629966 = Join.apply(frame1=AmazonS3_node1752274256074, frame2=Customer_trusted_node1752274558374, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1752274629966")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=Join_node1752274629966, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1752274246269", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1752274685240 = glueContext.getSink(path="s3://datalake71125/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1752274685240")
accelerometer_trusted_node1752274685240.setCatalogInfo(catalogDatabase="datalake71125",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1752274685240.setFormat("json")
accelerometer_trusted_node1752274685240.writeFrame(Join_node1752274629966)
job.commit()