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

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://ingested-json-ecommerce-dataset-fiap-grupo-c"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("﻿BibNum", "string", "﻿BibNum", "string"),
        ("Title", "string", "Title", "string"),
        ("Author", "string", "Author", "string"),
        ("ISBN", "string", "ISBN", "string"),
        ("PublicationYear", "string", "PublicationYear", "string"),
        ("Publisher", "string", "Publisher", "string"),
        ("Subjects", "string", "Subjects", "string"),
        ("ItemType", "string", "ItemType", "string"),
        ("ItemCollection", "string", "ItemCollection", "string"),
        ("FloatingItem", "string", "FloatingItem", "string"),
        ("ItemLocation", "string", "ItemLocation", "string"),
        ("ReportDate", "string", "ReportDate", "string"),
        ("ItemCount", "string", "ItemCount", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://parquet-ecommerce-dataset-fiap-grupo-c",
        "partitionKeys": [],
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="S3bucket_node3",
)

job.commit()
