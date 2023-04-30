import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer_Curated
Customer_Curated_node1682867864640 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://madyecustomerlanding/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="Customer_Curated_node1682867864640",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1682693574962 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://madyecustomerlanding/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1682693574962",
)

# Script generated for node Step Trainer OG
StepTrainerOG_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://madyecustomerlanding/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerOG_node1",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1682693505418 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://madyecustomerlanding/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1682693505418",
)

# Script generated for node Join
Join_node1682693470097 = Join.apply(
    frame1=CustomerTrusted_node1682693505418,
    frame2=AccelerometerTrusted_node1682693574962,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1682693470097",
)

# Script generated for node Join
Join_node1682693743271 = Join.apply(
    frame1=StepTrainerOG_node1,
    frame2=Join_node1682693470097,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1682693743271",
)

# Script generated for node Drop Fields
DropFields_node1682693797600 = DropFields.apply(
    frame=Join_node1682693743271,
    paths=[
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "user",
        "timeStamp",
        "x",
        "y",
        "z",
    ],
    transformation_ctx="DropFields_node1682693797600",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1682693797600,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://madyecustomerlanding/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
