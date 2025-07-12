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

# Script generated for node step trainer landing
steptrainerlanding_node1752283140250 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://datalake71125/step_trainer/landing/"], "recurse": True}, transformation_ctx="steptrainerlanding_node1752283140250")

# Script generated for node customer curated
customercurated_node1752283141850 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://datalake71125/customer/curated"], "recurse": True}, transformation_ctx="customercurated_node1752283141850")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1752283435224 = ApplyMapping.apply(frame=steptrainerlanding_node1752283140250, mappings=[("sensorreadingtime", "long", "right_sensorreadingtime", "long"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "right_distancefromobject", "int")], transformation_ctx="RenamedkeysforJoin_node1752283435224")

# Script generated for node Join
Join_node1752283422040 = Join.apply(frame1=customercurated_node1752283141850, frame2=RenamedkeysforJoin_node1752283435224, keys1=["serialnumber"], keys2=["right_serialnumber"], transformation_ctx="Join_node1752283422040")

# Script generated for node step trainer trusted
EvaluateDataQuality().process_rows(frame=Join_node1752283422040, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1752282689290", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
steptrainertrusted_node1752283614761 = glueContext.getSink(path="s3://datalake71125/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="steptrainertrusted_node1752283614761")
steptrainertrusted_node1752283614761.setCatalogInfo(catalogDatabase="datalake71125",catalogTableName="step trainer trusted")
steptrainertrusted_node1752283614761.setFormat("json")
steptrainertrusted_node1752283614761.writeFrame(Join_node1752283422040)
job.commit()