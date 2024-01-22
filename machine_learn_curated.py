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

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1705847302520 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="AccelerometerTrustedZone_node1705847302520",
    )
)

# Script generated for node Step Trainer Landing Zone
StepTrainerLandingZone_node1705843259292 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="step_trainer_landing",
        transformation_ctx="StepTrainerLandingZone_node1705843259292",
    )
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1705846670263 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1705846670263",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1705847158045 = ApplyMapping.apply(
    frame=CustomerTrustedZone_node1705846670263,
    mappings=[
        ("customername", "string", "customername", "string"),
        ("email", "string", "email", "string"),
        ("phone", "string", "phone", "string"),
        ("birthday", "string", "birthday", "string"),
        ("serialnumber", "string", "customer_serialnumber", "string"),
        ("registrationdate", "long", "registrationdate", "long"),
        ("lastupdatedate", "long", "lastupdatedate", "long"),
        ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"),
        ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"),
        ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1705847158045",
)

# Script generated for node SQL Query
SqlQuery2160 = """
select st.* from st
join c on c.customer_serialnumber = st.serialnumber
join a on st.sensorreadingtime = a.timestamp
"""
SQLQuery_node1705847335524 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2160,
    mapping={
        "a": AccelerometerTrustedZone_node1705847302520,
        "st": StepTrainerLandingZone_node1705843259292,
        "c": RenamedkeysforJoin_node1705847158045,
    },
    transformation_ctx="SQLQuery_node1705847335524",
)

# Script generated for node Amazon S3
AmazonS3_node1705843669967 = glueContext.getSink(
    path="s3://glue-bucket-learn/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1705843669967",
)
AmazonS3_node1705843669967.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
AmazonS3_node1705843669967.setFormat("json")
AmazonS3_node1705843669967.writeFrame(SQLQuery_node1705847335524)
job.commit()
