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

# Script generated for node Customer Landing
CustomerLanding_node1705329150730 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-233718526883/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1705329150730",
)

# Script generated for node privacyFilter
privacyFilter_node1705329180037 = Filter.apply(
    frame=CustomerLanding_node1705329150730,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="privacyFilter_node1705329180037",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1705329250602 = glueContext.write_dynamic_frame.from_options(
    frame=privacyFilter_node1705329180037,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-233718526883/customer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node1705329250602",
)

job.commit()
