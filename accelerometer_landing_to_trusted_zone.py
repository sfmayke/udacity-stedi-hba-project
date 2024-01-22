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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1705777976977 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1705777976977",
)

# Script generated for node Customer Trusted Table
CustomerTrustedTable_node1705777859243 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedTable_node1705777859243",
)

# Script generated for node Join
Join_node1705778014834 = Join.apply(
    frame1=AccelerometerLanding_node1705777976977,
    frame2=CustomerTrustedTable_node1705777859243,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1705778014834",
)

# Script generated for node Change Schema
ChangeSchema_node1705781744554 = ApplyMapping.apply(
    frame=Join_node1705778014834,
    mappings=[
        ("user", "string", "user", "string"),
        ("timestamp", "long", "timestamp", "long"),
        ("x", "float", "x", "float"),
        ("y", "float", "y", "float"),
        ("z", "float", "z", "float"),
    ],
    transformation_ctx="ChangeSchema_node1705781744554",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705778099568 = glueContext.getSink(
    path="s3://glue-bucket-learn/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1705778099568",
)
AccelerometerTrusted_node1705778099568.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1705778099568.setFormat("json")
AccelerometerTrusted_node1705778099568.writeFrame(ChangeSchema_node1705781744554)
job.commit()
