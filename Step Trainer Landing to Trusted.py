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

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1705851356403 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerCuratedZone_node1705851356403",
)

# Script generated for node Step Trainer Landing Zone
StepTrainerLandingZone_node1705849759820 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="step_trainer_landing",
        transformation_ctx="StepTrainerLandingZone_node1705849759820",
    )
)

# Script generated for node SQL Query
SqlQuery2234 = """
select st.* from c
join st on c.serialnumber = st.serialnumber
"""
SQLQuery_node1705851581531 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2234,
    mapping={
        "c": CustomerCuratedZone_node1705851356403,
        "st": StepTrainerLandingZone_node1705849759820,
    },
    transformation_ctx="SQLQuery_node1705851581531",
)

# Script generated for node Amazon S3
AmazonS3_node1705852307863 = glueContext.getSink(
    path="s3://glue-bucket-learn/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1705852307863",
)
AmazonS3_node1705852307863.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
AmazonS3_node1705852307863.setFormat("json")
AmazonS3_node1705852307863.writeFrame(SQLQuery_node1705851581531)
job.commit()
