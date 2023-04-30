import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://madyecustomerlanding/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1682867387150 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://madyecustomerlanding/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1682867387150",
)

# Script generated for node Join
Join_node1682867431938 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AccelerometerTrusted_node1682867387150,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1682867431938",
)

# Script generated for node Drop Fields
DropFields_node1682867570609 = DropFields.apply(
    frame=Join_node1682867431938,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1682867570609",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1682867567621 = DynamicFrame.fromDF(
    DropFields_node1682867570609.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1682867567621",
)

# Script generated for node Customer_Curated
Customer_Curated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1682867567621,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://madyecustomerlanding/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="Customer_Curated_node3",
)

job.commit()
