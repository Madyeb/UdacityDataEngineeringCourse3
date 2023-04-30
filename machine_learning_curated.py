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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1682700924519 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://madyecustomerlanding/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1682700924519",
)

# Script generated for node accelerometer
accelerometer_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://madyecustomerlanding/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_node1",
)

# Script generated for node Customer_trusted
Customer_trusted_node1682700954032 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://madyecustomerlanding/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Customer_trusted_node1682700954032",
)

# Script generated for node Join
Join_node1682700985505 = Join.apply(
    frame1=Customer_trusted_node1682700954032,
    frame2=accelerometer_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1682700985505",
)

# Script generated for node Join
Join_node1682701591036 = Join.apply(
    frame1=Join_node1682700985505,
    frame2=step_trainer_trusted_node1682700924519,
    keys1=["serialNumber", "timeStamp"],
    keys2=["serialNumber", "sensorReadingTime"],
    transformation_ctx="Join_node1682701591036",
)

# Script generated for node Drop Fields
DropFields_node1682701625266 = DropFields.apply(
    frame=Join_node1682701591036,
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
    ],
    transformation_ctx="DropFields_node1682701625266",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1682701625266,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://madyecustomerlanding/SQLQueries/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
