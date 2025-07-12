import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1752259082675 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://datalake71125/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1752259082675")

# Script generated for node Change Schema
ChangeSchema_node1752273404177 = ApplyMapping.apply(frame=AmazonS3_node1752259082675, mappings=[("customername", "string", "customername", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthday", "string", "birthday", "string"), ("serialnumber", "string", "serialnumber", "string"), ("registrationdate", "long", "registrationdate", "long"), ("lastupdatedate", "long", "lastupdatedate", "long"), ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "int"), ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"), ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long")], transformation_ctx="ChangeSchema_node1752273404177")

# Script generated for node Filter Nulls 
FilterNulls_node1752273220875 = Filter.apply(frame=ChangeSchema_node1752273404177, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="FilterNulls_node1752273220875")

# Script generated for node Customer Trusted
CustomerTrusted_node1752259275816 = glueContext.getSink(path="s3://datalake71125/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1752259275816")
CustomerTrusted_node1752259275816.setCatalogInfo(catalogDatabase="datalake71125",catalogTableName="customer_trusted")
CustomerTrusted_node1752259275816.setFormat("json")
CustomerTrusted_node1752259275816.writeFrame(FilterNulls_node1752273220875)
job.commit()