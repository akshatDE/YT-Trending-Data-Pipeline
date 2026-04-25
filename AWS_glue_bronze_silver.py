"""
Glue Job: Bronze → Silver (Statistics + Reference Data)
────────────────────────────────────────────────────────
Reads raw CSV/JSON statistics AND category reference JSON
from the Bronze layer, applies schema enforcement, cleansing,
deduplication, and writes both clean Parquet tables to Silver.

Job Parameters:
    --JOB_NAME                   — Glue job name (auto-set)
    --bronze_database            — Bronze Glue catalog database
    --bronze_stats_table         — Bronze statistics table
    --bronze_reference_table     — Bronze reference/category table
    --silver_bucket              — Silver S3 bucket
    --silver_database            — Silver Glue catalog database
    --silver_stats_table         — Silver statistics table
    --silver_reference_table     — Silver reference table
"""

import sys
import boto3
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, BooleanType
from pyspark.sql.window import Window

# ── Job Setup ────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_database",
    "bronze_stats_table",
    "bronze_reference_table",
    "silver_bucket",
    "silver_database",
    "silver_stats_table",
    "silver_reference_table",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# ── Config ───────────────────────────────────────────────────────────────────
BRONZE_DB           = args["bronze_database"]
BRONZE_STATS_TABLE  = args["bronze_stats_table"]
BRONZE_REF_TABLE    = args["bronze_reference_table"]
SILVER_BUCKET       = args["silver_bucket"]
SILVER_DB           = args["silver_database"]
SILVER_STATS_TABLE  = args["silver_stats_table"]
SILVER_REF_TABLE    = args["silver_reference_table"]
SILVER_STATS_PATH   = f"s3://{SILVER_BUCKET}/youtube/statistics/"
SILVER_REF_PATH     = f"s3://{SILVER_BUCKET}/youtube/reference/"

logger.info(f"Bronze DB: {BRONZE_DB}")
logger.info(f"Silver DB: {SILVER_DB}")


# ── Ensure Silver Database Exists ────────────────────────────────────────────
glue_client = boto3.client("glue")
try:
    glue_client.get_database(Name=SILVER_DB)
    logger.info(f"Database {SILVER_DB} confirmed.")
except glue_client.exceptions.EntityNotFoundException:
    glue_client.create_database(DatabaseInput={"Name": SILVER_DB})
    logger.info(f"Database {SILVER_DB} created.")


# ════════════════════════════════════════════════════════════════════════════
# PART 1 — STATISTICS
# ════════════════════════════════════════════════════════════════════════════

logger.info("=" * 60)
logger.info("PART 1: Processing Statistics Data")
logger.info("=" * 60)

# ── Step 1: Read Stats from Bronze ───────────────────────────────────────────
predicate = "region in ('CA','GB','US','IN')"

stats_source = glueContext.create_dynamic_frame.from_catalog(
    database=BRONZE_DB,
    table_name=BRONZE_STATS_TABLE,
    transformation_ctx="stats_source",
    push_down_predicate=predicate,
)

df_stats = stats_source.toDF()
stats_initial_count = df_stats.count()
logger.info(f"Bronze stats records read: {stats_initial_count}")

if stats_initial_count == 0:
    logger.info("No stats records to process.")
else:
    # ── Step 2: Schema Enforcement ───────────────────────────────────────────
    columns = set(df_stats.columns)

    if "snippet.title" in columns or "snippet__title" in columns:
        logger.info("Detected YouTube API format — flattening...")
        df_stats = df_stats.select(
            F.col("id").alias("video_id"),
            F.lit(datetime.utcnow().strftime("%y.%d.%m")).alias("trending_date"),
            F.col("snippet__title").alias("title"),
            F.col("snippet__channelTitle").alias("channel_title"),
            F.col("snippet__categoryId").cast(LongType()).alias("category_id"),
            F.col("snippet__publishedAt").alias("publish_time"),
            F.col("snippet__tags").alias("tags") if "snippet__tags" in columns
                else F.lit(None).cast(StringType()).alias("tags"),
            F.col("statistics__viewCount").cast(LongType()).alias("views"),
            F.col("statistics__likeCount").cast(LongType()).alias("likes"),
            F.lit(0).cast(LongType()).alias("dislikes"),
            F.col("statistics__commentCount").cast(LongType()).alias("comment_count"),
            F.lit(None).cast(StringType()).alias("thumbnail_link"),
            F.lit(False).alias("comments_disabled"),
            F.lit(False).alias("ratings_disabled"),
            F.lit(False).alias("video_error_or_removed"),
            F.col("snippet__description").alias("description"),
            F.col("region"),
        )
    else:
        logger.info("Detected Kaggle CSV format — casting types...")
        df_stats = df_stats.select(
            F.col("video_id").cast(StringType()),
            F.col("trending_date").cast(StringType()),
            F.col("title").cast(StringType()),
            F.col("channel_title").cast(StringType()),
            F.col("category_id").cast(LongType()),
            F.col("publish_time").cast(StringType()),
            F.col("tags").cast(StringType()),
            F.col("views").cast(LongType()),
            F.col("likes").cast(LongType()),
            F.col("dislikes").cast(LongType()),
            F.col("comment_count").cast(LongType()),
            F.col("thumbnail_link").cast(StringType()),
            F.col("comments_disabled").cast(BooleanType()),
            F.col("ratings_disabled").cast(BooleanType()),
            F.col("video_error_or_removed").cast(BooleanType()),
            F.col("description").cast(StringType()),
            F.col("region").cast(StringType()),
        )

    # ── Step 3: Cleansing ────────────────────────────────────────────────────
    df_stats = df_stats.filter(F.col("video_id").isNotNull())
    df_stats = df_stats.withColumn("region", F.lower(F.trim(F.col("region"))))
    df_stats = df_stats.withColumn(
        "trending_date_parsed",
        F.when(
            F.col("trending_date").rlike(r"^\d{2}\.\d{2}\.\d{2}$"),
            F.to_date(F.col("trending_date"), "yy.dd.MM")
        ).otherwise(F.to_date(F.col("trending_date")))
    )
    for col_name in ["views", "likes", "dislikes", "comment_count"]:
        df_stats = df_stats.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))

    df_stats = df_stats.withColumn("like_ratio",
        F.when(F.col("views") > 0,
            F.round(F.col("likes") / F.col("views") * 100, 4)
        ).otherwise(0.0)
    )
    df_stats = df_stats.withColumn("engagement_rate",
        F.when(F.col("views") > 0,
            F.round((F.col("likes") + F.col("dislikes") + F.col("comment_count")) / F.col("views") * 100, 4)
        ).otherwise(0.0)
    )
    df_stats = df_stats.withColumn("_processed_at", F.current_timestamp())
    df_stats = df_stats.withColumn("_job_name", F.lit(args["JOB_NAME"]))

    # ── Step 4: Deduplication ────────────────────────────────────────────────
    window = Window.partitionBy("video_id", "region", "trending_date_parsed") \
        .orderBy(F.col("_processed_at").desc())
    df_stats = df_stats.withColumn("_row_num", F.row_number().over(window)) \
        .filter(F.col("_row_num") == 1).drop("_row_num")

    stats_clean_count = df_stats.count()
    logger.info(f"Stats after cleansing & dedup: {stats_clean_count} records")

    # ── Step 5: DQ Checks ────────────────────────────────────────────────────
    for col_name in ["video_id", "title", "channel_title", "views"]:
        null_count = df_stats.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            logger.warn(f"DQ WARNING: {col_name} has {null_count} nulls")

    # ── Step 6: Write Stats to Silver ────────────────────────────────────────
    logger.info(f"Writing stats to Silver: {SILVER_STATS_PATH}")

    stats_frame = DynamicFrame.fromDF(df_stats, glueContext, "silver_statistics")
    stats_sink = glueContext.getSink(
        connection_type="s3",
        path=SILVER_STATS_PATH,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["region"],
    )
    stats_sink.setCatalogInfo(catalogDatabase=SILVER_DB, catalogTableName=SILVER_STATS_TABLE)
    stats_sink.setFormat("glueparquet", compression="snappy")
    stats_sink.writeFrame(stats_frame)
    logger.info(f"Stats write complete: {stats_clean_count} records.")


# ════════════════════════════════════════════════════════════════════════════
# PART 2 — REFERENCE DATA
# ════════════════════════════════════════════════════════════════════════════

logger.info("=" * 60)
logger.info("PART 2: Processing Reference Data")
logger.info("=" * 60)

# ── Step 1: Read Reference from Bronze ───────────────────────────────────────
ref_source = glueContext.create_dynamic_frame.from_catalog(
    database=BRONZE_DB,
    table_name=BRONZE_REF_TABLE,
    transformation_ctx="ref_source",
)

df_ref = ref_source.toDF()
ref_initial_count = df_ref.count()
logger.info(f"Bronze reference records read: {ref_initial_count}")
logger.info(f"Reference columns: {df_ref.columns}")

if ref_initial_count == 0:
    logger.info("No reference records to process.")
else:
    # ── Step 2: Schema Enforcement ───────────────────────────────────────────
# items is an array of structs — explode first, then extract fields
    df_ref = df_ref.select(
        F.explode(F.col("items")).alias("item"),
        F.col("region")
    )
    
    df_ref = df_ref.select(
        F.col("item.id").cast(LongType()).alias("category_id"),
        F.col("item.snippet.title").cast(StringType()).alias("category_title"),
        F.col("item.snippet.assignable").cast(BooleanType()).alias("assignable"),
        F.col("item.kind").cast(StringType()).alias("kind"),
        F.col("region").cast(StringType()).alias("region"),
    )

    # ── Step 3: Cleansing ────────────────────────────────────────────────────
    df_ref = df_ref.filter(F.col("category_id").isNotNull())
    df_ref = df_ref.withColumn("region", F.lower(F.trim(F.col("region"))))
    df_ref = df_ref.withColumn("category_title", F.trim(F.col("category_title")))
    df_ref = df_ref.withColumn("_processed_at", F.current_timestamp())
    df_ref = df_ref.withColumn("_job_name", F.lit(args["JOB_NAME"]))

    # ── Step 4: Deduplication ────────────────────────────────────────────────
    window_ref = Window.partitionBy("category_id", "region") \
        .orderBy(F.col("_processed_at").desc())
    df_ref = df_ref.withColumn("_row_num", F.row_number().over(window_ref)) \
        .filter(F.col("_row_num") == 1).drop("_row_num")

    ref_clean_count = df_ref.count()
    logger.info(f"Reference after cleansing & dedup: {ref_clean_count} records")

    # ── Step 5: DQ Checks ────────────────────────────────────────────────────
    for col_name in ["category_id", "category_title", "region"]:
        null_count = df_ref.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            logger.warn(f"DQ WARNING: {col_name} has {null_count} nulls")

    # ── Step 6: Write Reference to Silver ────────────────────────────────────
    logger.info(f"Writing reference to Silver: {SILVER_REF_PATH}")

    ref_frame = DynamicFrame.fromDF(df_ref, glueContext, "silver_reference")
    ref_sink = glueContext.getSink(
        connection_type="s3",
        path=SILVER_REF_PATH,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["region"],
    )
    ref_sink.setCatalogInfo(catalogDatabase=SILVER_DB, catalogTableName=SILVER_REF_TABLE)
    ref_sink.setFormat("glueparquet", compression="snappy")
    ref_sink.writeFrame(ref_frame)
    logger.info(f"Reference write complete: {ref_clean_count} records.")


job.commit()