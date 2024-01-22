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

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1705842629085 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1705842629085",
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1705842743545 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="AccelerometerTrustedZone_node1705842743545",
    )
)

# Script generated for node Join
Join_node1705842816786 = Join.apply(
    frame1=CustomerTrustedZone_node1705842629085,
    frame2=AccelerometerTrustedZone_node1705842743545,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1705842816786",
)

# Script generated for node Drop Fields
DropFields_node1705844374437 = DropFields.apply(
    frame=Join_node1705842816786,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1705844374437",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1705845983696 = DynamicFrame.fromDF(
    DropFields_node1705844374437.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1705845983696",
)

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1705842846797 = glueContext.getSink(
    path="s3://glue-bucket-learn/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCuratedZone_node1705842846797",
)
CustomerCuratedZone_node1705842846797.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCuratedZone_node1705842846797.setFormat("json")
CustomerCuratedZone_node1705842846797.writeFrame(DropDuplicates_node1705845983696)
job.commit()
