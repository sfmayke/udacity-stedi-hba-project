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

# Script generated for node Amazon S3
AmazonS3_node1705686505570 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://glue-bucket-learn/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1705686505570",
)

# Script generated for node Privacy Filter
SqlQuery2420 = """
SELECT * 
FROM customer_data
WHERE shareWithResearchAsOfDate != 0
"""
PrivacyFilter_node1705688637717 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2420,
    mapping={"customer_data": AmazonS3_node1705686505570},
    transformation_ctx="PrivacyFilter_node1705688637717",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1705686693395 = glueContext.getSink(
    path="s3://glue-bucket-learn/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1705686693395",
)
CustomerTrusted_node1705686693395.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
CustomerTrusted_node1705686693395.setFormat("json")
CustomerTrusted_node1705686693395.writeFrame(PrivacyFilter_node1705688637717)
job.commit()
