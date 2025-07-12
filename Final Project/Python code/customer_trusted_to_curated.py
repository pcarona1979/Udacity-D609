import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node customer trusted
customertrusted_node1752281161404 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://datalake71125/customer/trusted/"], "recurse": True}, transformation_ctx="customertrusted_node1752281161404")

# Script generated for node accelerometer trusted
accelerometertrusted_node1752281212954 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://datalake71125/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometertrusted_node1752281212954")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1752281395184 = ApplyMapping.apply(frame=customertrusted_node1752281161404, mappings=[("registrationdate", "long", "right_registrationdate", "long"), ("customername", "string", "right_customername", "string"), ("birthday", "string", "right_birthday", "string"), ("lastupdatedate", "long", "right_lastupdatedate", "long"), ("email", "string", "right_email", "string"), ("serialnumber", "string", "right_serialnumber", "string"), ("phone", "string", "right_phone", "string"), ("sharewithresearchasofdate", "int", "right_sharewithresearchasofdate", "int"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long")], transformation_ctx="RenamedkeysforJoin_node1752281395184")

# Script generated for node Join
Join_node1752281337641 = Join.apply(frame1=accelerometertrusted_node1752281212954, frame2=RenamedkeysforJoin_node1752281395184, keys1=["email"], keys2=["right_email"], transformation_ctx="Join_node1752281337641")

# Script generated for node Change Schema
ChangeSchema_node1752281435118 = ApplyMapping.apply(frame=Join_node1752281337641, mappings=[("right_birthday", "string", "right_birthday", "string"), ("registrationdate", "long", "registrationdate", "long"), ("customername", "string", "customername", "string"), ("right_email", "string", "right_email", "string"), ("right_sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("birthday", "string", "birthday", "string"), ("right_sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long"), ("right_sharewithresearchasofdate", "int", "right_sharewithresearchasofdate", "int"), ("right_phone", "string", "right_phone", "string"), ("right_serialnumber", "string", "right_serialnumber", "string"), ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long"), ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"), ("right_registrationdate", "long", "right_registrationdate", "long"), ("lastupdatedate", "long", "lastupdatedate", "long"), ("right_customername", "string", "right_customername", "string"), ("right_lastupdatedate", "long", "right_lastupdatedate", "long"), ("email", "string", "email", "string"), ("serialnumber", "string", "serialnumber", "string"), ("phone", "string", "phone", "string"), ("sharewithresearchasofdate", "int", "sharewithresearchasofdate", "int")], transformation_ctx="ChangeSchema_node1752281435118")

# Script generated for node Drop Duplicates
DropDuplicates_node1752282079652 =  DynamicFrame.fromDF(ChangeSchema_node1752281435118.toDF().dropDuplicates(["customername"]), glueContext, "DropDuplicates_node1752282079652")

# Script generated for node customer curated
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1752282079652, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1752280771761", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customercurated_node1752281648694 = glueContext.getSink(path="s3://datalake71125/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customercurated_node1752281648694")
customercurated_node1752281648694.setCatalogInfo(catalogDatabase="datalake71125",catalogTableName="customer_curated")
customercurated_node1752281648694.setFormat("json")
customercurated_node1752281648694.writeFrame(DropDuplicates_node1752282079652)
job.commit()