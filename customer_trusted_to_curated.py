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

# Script generated for node Accelerometer landing
Accelerometerlanding_node1705330585672 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-233718526883/accelerometer_landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometerlanding_node1705330585672",
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
    frame2=Accelerometerlanding_node1705330585672,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1705330625854",
)

# Script generated for node Drop Fields
DropFields_node1705330900984 = DropFields.apply(
    frame=Join_node1705330625854,
    paths=["z", "y", "x", "timestamp", "user"],
    transformation_ctx="DropFields_node1705330900984",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1705338204062 = DynamicFrame.fromDF(
    DropFields_node1705330900984.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1705338204062",
)

# Script generated for node Customer Curated
CustomerCurated_node1705330716448 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1705338204062,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-233718526883/customer_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node1705330716448",
)

job.commit()
