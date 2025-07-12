import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

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

# Script generated for node step trainer trusted
steptrainertrusted_node1752284292029 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://datalake71125/step_trainer/trusted"], "recurse": True}, transformation_ctx="steptrainertrusted_node1752284292029")

# Script generated for node accelerometer trusted
accelerometertrusted_node1752284293550 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://datalake71125/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometertrusted_node1752284293550")

# Script generated for node Change Schema
ChangeSchema_node1752285341018 = ApplyMapping.apply(frame=steptrainertrusted_node1752284292029, mappings=[("right_distancefromobject", "int", "right_distancefromobject", "int"), ("right_sensorreadingtime", "long", "right_sensorreadingtime", "int"), ("registrationdate", "long", "registrationdate", "long"), ("customername", "string", "customername", "string"), ("lastupdatedate", "long", "lastupdatedate", "long"), ("email", "string", "email", "string"), ("serialnumber", "string", "serialnumber", "string"), ("phone", "string", "phone", "string"), ("sharewithresearchasofdate", "int", "sharewithresearchasofdate", "int")], transformation_ctx="ChangeSchema_node1752285341018")

# Script generated for node Change Schema
ChangeSchema_node1752285130005 = ApplyMapping.apply(frame=accelerometertrusted_node1752284293550, mappings=[("z", "double", "z", "double"), ("registrationdate", "long", "registrationdate", "long"), ("customername", "string", "customername", "string"), ("user", "string", "user", "string"), ("y", "double", "y", "double"), ("x", "double", "x", "double"), ("timestamp", "long", "timestamp", "int"), ("lastupdatedate", "long", "lastupdatedate", "long"), ("email", "string", "email", "string"), ("serialnumber", "string", "serialnumber", "string"), ("sharewithresearchasofdate", "int", "sharewithresearchasofdate", "int"), ("phone", "string", "phone", "string")], transformation_ctx="ChangeSchema_node1752285130005")

# Script generated for node Join
ChangeSchema_node1752285130005DF = ChangeSchema_node1752285130005.toDF()
ChangeSchema_node1752285341018DF = ChangeSchema_node1752285341018.toDF()
Join_node1752284491895 = DynamicFrame.fromDF(ChangeSchema_node1752285130005DF.join(ChangeSchema_node1752285341018DF, (ChangeSchema_node1752285130005DF['timestamp'] == ChangeSchema_node1752285341018DF['right_sensorreadingtime']), "outer"), glueContext, "Join_node1752284491895")

# Script generated for node machine learning curated
EvaluateDataQuality().process_rows(frame=Join_node1752284491895, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1752284275130", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machinelearningcurated_node1752285758566 = glueContext.getSink(path="s3://datalake71125/machine_learning/curated/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machinelearningcurated_node1752285758566")
machinelearningcurated_node1752285758566.setCatalogInfo(catalogDatabase="datalake71125",catalogTableName="machine learning curated")
machinelearningcurated_node1752285758566.setFormat("json")
machinelearningcurated_node1752285758566.writeFrame(Join_node1752284491895)
job.commit()