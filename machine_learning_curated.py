import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


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

# Script generated for node SQL Query
SqlQuery1263 = """
select * from AT join ST on ST.sensorreadingtime = AT.timestamp

"""
SQLQuery_node1705405684720 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1263,
    mapping={
        "AT": AccelerometerTrusted_node1705396797899,
        "ST": Steptrainertrusted_node1705396843765,
    },
    transformation_ctx="SQLQuery_node1705405684720",
)

# Script generated for node Amazon S3
AmazonS3_node1705397051192 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1705405684720,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-233718526883/ML_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1705397051192",
)

job.commit()
