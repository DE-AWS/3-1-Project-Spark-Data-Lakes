import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1705330585672 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-233718526883/accelerometer_landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1705330585672",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1705330507985 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-233718526883/customer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1705330507985",
)

# Script generated for node Join
Join_node1705330625854 = Join.apply(
    frame1=CustomerTrusted_node1705330507985,
    frame2=AccelerometerLanding_node1705330585672,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1705330625854",
)

# Script generated for node Filter
Filter_node1705330684509 = Filter.apply(
    frame=Join_node1705330625854,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1705330684509",
)

# Script generated for node Drop Fields
DropFields_node1705330900984 = DropFields.apply(
    frame=Filter_node1705330684509,
    paths=[
        "serialNumber",
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1705330900984",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705330716448 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1705330900984,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-233718526883/accelometer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1705330716448",
)

job.commit()
