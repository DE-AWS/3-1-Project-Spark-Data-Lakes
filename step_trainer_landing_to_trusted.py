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

# Script generated for node Customer Curated
CustomerCurated_node1705331601986 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-233718526883/customer_curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1705331601986",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1705331629748 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project-233718526883/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1705331629748",
)

# Script generated for node join
SqlQuery1116 = """
select * from cc join stl on cc.serialnumber = stl.serialnumber
"""
join_node1705404516688 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1116,
    mapping={
        "cc": CustomerCurated_node1705331601986,
        "stl": StepTrainerLanding_node1705331629748,
    },
    transformation_ctx="join_node1705404516688",
)

# Script generated for node SQL Query
SqlQuery1117 = """
select distinct serialnumber,sensorreadingtime,distancefromobject 
from myDataSource
"""
SQLQuery_node1705402532909 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1117,
    mapping={"myDataSource": join_node1705404516688},
    transformation_ctx="SQLQuery_node1705402532909",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1705332255141 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1705402532909,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-233718526883/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1705332255141",
)

job.commit()
