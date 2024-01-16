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

# Script generated for node Join customer
Joincustomer_node1705333198164 = Join.apply(
    frame1=CustomerCurated_node1705331601986,
    frame2=StepTrainerLanding_node1705331629748,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Joincustomer_node1705333198164",
)

# Script generated for node Drop Fields
DropFields_node1705331814221 = DropFields.apply(
    frame=Joincustomer_node1705333198164,
    paths=[
        "email",
        "phone",
        "birthday",
        "sharewithresearchasofdate",
        "registrationdate",
        "customername",
        "lastupdatedate",
        "`.serialnumber`",
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1705331814221",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1705340831901 = DynamicFrame.fromDF(
    DropFields_node1705331814221.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1705340831901",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1705332255141 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1705340831901,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project-233718526883/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1705332255141",
)

job.commit()
