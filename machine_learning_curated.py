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

# Script generated for node Step trainer trusted
Steptrainertrusted_node1705396843765 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-233718526883/step_trainer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="Steptrainertrusted_node1705396843765",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705396797899 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-233718526883/accelometer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1705396797899",
)

# Script generated for node Join
Join_node1705396959518 = Join.apply(
    frame1=AccelerometerTrusted_node1705396797899,
    frame2=Steptrainertrusted_node1705396843765,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1705396959518",
)

# Script generated for node Amazon S3
AmazonS3_node1705397051192 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1705396959518,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-233718526883/ML_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1705397051192",
)

job.commit()
