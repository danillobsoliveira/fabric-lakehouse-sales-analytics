# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b112109a-51a8-4b2e-bfa2-df2d8f6fa1f3",
# META       "default_lakehouse_name": "lh_sales_analytics",
# META       "default_lakehouse_workspace_id": "b2bbdee6-3aaf-4818-9a42-64f24bc5c285",
# META       "known_lakehouses": [
# META         {
# META           "id": "b112109a-51a8-4b2e-bfa2-df2d8f6fa1f3"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 🥇 Gold Layer — Analytical Modeling
# 
# **Objective**
# 
# This notebook is responsible for transforming curated Silver data into
# analytical Gold data models following recognized industry patterns:
# 
# - Kimball Dimensional Modeling
# - Star Schema
# - Slowly Changing Dimensions (Type 2)
# - Append-only Fact Tables
# - Delta Lake MERGE semantics
# - Microsoft Fabric Lakehouse

# MARKDOWN ********************

# ## Runtime Configuration
# 
# This section defines runtime dependencies, execution parameters,
# and pipeline context required for the transformation process.

# CELL ********************

# ============================================================
# Runtime Dependencies
# ============================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, IntegerType, LongType

# ============================================================
# Execution Context
# ============================================================

ENV = "dev"
LAYER = "gold"
PIPELINE_NAME = "gold_analytical_modeling"

# ============================================================
# Orchestration Context
# ============================================================

batch_id = spark.conf.get("pipeline.batchId", "manual_run")
execution_timestamp = F.current_timestamp()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Schema Validation and Initialization
# 
# Validate the existence of Gold layer tables following a Kimball-style
# star schema. If tables do not exist, they are created to ensure a
# consistent and controlled analytical structure.
# 
# The model consists of one fact table and three conformed dimensions:
# Customer, Product, and Date.

# CELL ********************

# ============================================================
# Schema Validation and Initialization (Kimball Star Schema)
# ============================================================

# Create Gold database if it does not exist
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# ------------------------------------------------------------
# Dimension: Customer
# ------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_sk BIGINT,
    customer_id BIGINT,
    customer_name STRING,
    customer_email STRING,
    hash_diff STRING,
    is_current BOOLEAN,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    batch_id STRING
)
USING DELTA
""")

# ------------------------------------------------------------
# Dimension: Product
# ------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_sk BIGINT,
    product_id BIGINT,
    product_name STRING,
    product_category STRING,
    hash_diff STRING,
    is_current BOOLEAN,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    batch_id STRING
)
USING DELTA
""")

# ------------------------------------------------------------
# Dimension: Date
# ------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_sk INT,
    full_date DATE,
    year INT,
    month INT,
    month_name STRING,
    quarter INT,
    day INT,
    day_of_week STRING,
    is_weekend BOOLEAN
)
USING DELTA
""")

# ------------------------------------------------------------
# Fact: Purchases
# ------------------------------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS gold.fact_purchases (
    purchase_id BIGINT,
    customer_sk BIGINT,
    product_sk BIGINT,
    date_sk INT,
    quantity INT,
    unit_price DECIMAL(12,2),
    total_amount DECIMAL(14,2),
    batch_id STRING
)
USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Silver Data
# 
# Load curated datasets from the Silver layer to serve as the foundation
# for analytical modeling in the Gold layer.
# 
# These datasets are assumed to be clean, standardized, and validated,
# allowing the Gold layer to focus on business logic and dimensional design.

# CELL ********************

# ============================================================
# Load Silver Data
# ============================================================

# Load purchase-level data from Silver layer
df_silver_purchases = spark.table("silver.purchases")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Date Dimension
# 
# This section builds and maintains the **Date Dimension** in the Gold layer following
# **Kimball best practices**.
# 
# The Date Dimension uses a **deterministic surrogate key (`date_sk`)** in the `YYYYMMDD`
# format and follows an **insert-only strategy**, as date attributes do not change over
# time.

# CELL ********************

# Derive distinct dates from Silver purchases
df_dim_date_prepared = (
    df_silver_purchases
        .select(F.col("purchase_date").alias("full_date"))
        .distinct()
        .withColumn("date_sk", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("year", F.year("full_date"))
        .withColumn("day_of_week", F.date_format(F.col("full_date"), "E"))
        .withColumn(
            "is_weekend",
            F.dayofweek("full_date").isin(1, 7)
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load Delta table reference
dim_date = DeltaTable.forName(spark, "gold.dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Insert new dates only
(
    dim_date.alias("tgt")
        .merge(
            df_dim_date_prepared.alias("src"),
            "tgt.date_sk = src.date_sk"
        )
        .whenNotMatchedInsert(
            values={
                "date_sk": "src.date_sk",
                "full_date": "src.full_date",
                "year": "src.year",
                "quarter": "src.quarter",
                "month": "src.month",
                "month_name": "src.month_name",
                "day": "src.day",
                "day_of_week": "src.day_of_week",
                "is_weekend": "src.is_weekend"
            }
        )
        .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Customer Dimension (SCD Type 2)
# 
# This section is responsible for building and maintaining the **Customer Dimension**
# in the Gold layer using **Kimball modeling principles** and **Slowly Changing Dimension
# Type 2 (SCD2)**.
# 
# The process derives customer attributes from the Silver purchases dataset and applies
# a **hash-based change detection strategy** combined with **Delta Lake MERGE** to track
# historical changes while ensuring a single current record per customer.
# 
# Due to current limitations of **Microsoft Fabric Spark runtime**, surrogate keys cannot
# be generated using `IDENTITY` columns. Therefore, the surrogate key (`customer_sk`) is
# generated manually during the load process, ensuring uniqueness and idempotency within
# each batch execution.

# CELL ********************

# Select customer attributes from Silver layer
# Deduplicate by natural key
df_dim_customer_prepared = (
    df_silver_purchases
        .select(
            F.col("customer_id").alias("customer_nk"),
            F.col("customer_name"),
            F.col("customer_email")
        )
        .dropDuplicates(["customer_nk"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Attributes that drive historical changes
SCD2_COLUMNS_CUSTOMER = [
    "customer_name",
    "customer_email"
]

# Generate hash for change detection
df_dim_customer_prepared = (
    df_dim_customer_prepared
        .withColumn(
            "hash_diff",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[F.col(c) for c in SCD2_COLUMNS_CUSTOMER]
                ),
                256
            )
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Retrieve current maximum surrogate key
max_customer_sk = (
    spark.table("gold.dim_customer")
        .select(F.coalesce(F.max("customer_sk"), F.lit(0)).alias("max_sk"))
        .collect()[0]["max_sk"]
)

# Assign surrogate keys only to incoming records
df_dim_customer_prepared = (
    df_dim_customer_prepared
        .withColumn(
            "customer_sk",
            F.row_number().over(
                Window.orderBy("customer_nk")
            ) + F.lit(max_customer_sk)
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load Delta table reference
dim_customer = DeltaTable.forName(spark, "gold.dim_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge logic:
# - Match on natural key and current record
# - Close current record when changes are detected
# - Insert new records and new historical versions
(
    dim_customer.alias("tgt")
        .merge(
            df_dim_customer_prepared.alias("src"),
            """
            tgt.customer_id = src.customer_nk
            AND tgt.is_current = true
            """
        )
        .whenMatchedUpdate(
            condition="tgt.hash_diff <> src.hash_diff",
            set={
                "is_current": "false",
                "valid_to": "current_timestamp()"
            }
        )
        .whenNotMatchedInsert(
            values={
                "customer_sk": "src.customer_sk",
                "customer_id": "src.customer_nk",
                "customer_name": "src.customer_name",
                "customer_email": "src.customer_email",
                "hash_diff": "src.hash_diff",
                "valid_from": "current_timestamp()",
                "valid_to": "null",
                "is_current": "true",
                "batch_id": F.lit(batch_id)
            }
        )
        .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Product Dimension (SCD Type 2)
# 
# This section builds and maintains the **Product Dimension** in the Gold layer using
# **Kimball modeling** and **Slowly Changing Dimension Type 2 (SCD2)**.
# 
# Product attributes are derived from the Silver purchases dataset. Due to current
# limitations of the **Microsoft Fabric Spark runtime**, surrogate keys cannot be generated
# using `IDENTITY` columns and are therefore created manually during the load process.

# CELL ********************

# Select product attributes from Silver layer
# Deduplicate by natural key
df_dim_product_prepared = (
    df_silver_purchases
        .select(
            F.col("product_id").alias("product_nk"),
            F.col("product_name"),
            F.col("product_category")
        )
        .dropDuplicates(["product_nk"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Attributes that drive historical changes
SCD2_COLUMNS_PRODUCT = [
    "product_name",
    "product_category"
]

# Generate hash for change detection
df_dim_product_prepared = (
    df_dim_product_prepared
        .withColumn(
            "hash_diff",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[F.col(c) for c in SCD2_COLUMNS_PRODUCT]
                ),
                256
            )
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Retrieve current maximum surrogate key
max_product_sk = (
    spark.table("gold.dim_product")
        .select(F.coalesce(F.max("product_sk"), F.lit(0)).alias("max_sk"))
        .collect()[0]["max_sk"]
)

# Assign surrogate keys to incoming records
df_dim_product_prepared = (
    df_dim_product_prepared
        .withColumn(
            "product_sk",
            F.row_number().over(
                Window.orderBy("product_nk")
            ) + F.lit(max_product_sk)
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load Delta table reference
dim_product = DeltaTable.forName(spark, "gold.dim_product")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge logic:
# - Match on natural key and current record
# - Close record when changes are detected
# - Insert new records and new historical versions
(
    dim_product.alias("tgt")
        .merge(
            df_dim_product_prepared.alias("src"),
            """
            tgt.product_id = src.product_nk
            AND tgt.is_current = true
            """
        )
        .whenMatchedUpdate(
            condition="tgt.hash_diff <> src.hash_diff",
            set={
                "is_current": "false",
                "valid_to": "current_timestamp()"
            }
        )
        .whenNotMatchedInsert(
            values={
                "product_sk": "src.product_sk",
                "product_id": "src.product_nk",
                "product_name": "src.product_name",
                "product_category": "src.product_category",
                "hash_diff": "src.hash_diff",
                "valid_from": "current_timestamp()",
                "valid_to": "null",
                "is_current": "true",
                "batch_id": F.lit(batch_id)
            }
        )
        .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold – Fact Sales (Item-Level)
# 
# Tabela fato no nível mais granular (item da compra).  
# Responsável por registrar métricas quantitativas de vendas e relacioná-las às dimensões **Date**, **Customer** e **Product** por meio de **surrogate keys**.  
# A carga é **append-only**, garantindo imutabilidade e consistência analítica no modelo estrela.

# CELL ********************

# Base dataset from Silver layer
df_fact_base = (
    spark.table("silver.purchases")
        .select(
            "purchase_id",
            "purchase_date",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "item_total_value",
            "purchase_total",
            "source_system",
            "batch_id"
        )
        .alias("f")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Resolve date surrogate key
df_fact_date = (
    df_fact_base
        .join(
            spark.table("gold.dim_date")
                .select("date_sk", "full_date")
                .alias("d"),
            F.col("f.purchase_date") == F.col("d.full_date"),
            "left"
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Resolve customer surrogate key
df_fact_customer = (
    df_fact_date
        .join(
            spark.table("gold.dim_customer")
                .filter(F.col("is_current") == True)
                .select("customer_id", "customer_sk")
                .alias("c"),
            F.col("f.customer_id") == F.col("c.customer_id"),
            "left"
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Resolve product surrogate key
df_fact_enriched = (
    df_fact_customer
        .join(
            spark.table("gold.dim_product")
                .filter(F.col("is_current") == True)
                .select("product_id", "product_sk")
                .alias("p"),
            F.col("f.product_id") == F.col("p.product_id"),
            "left"
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Align Fact Columns to Target Schema
df_fact_aligned = (
    df_fact_enriched
        # Rename metric to match table contract
        .withColumnRenamed("item_total_value", "total_amount")

        # Cast measures
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
        .withColumn("total_amount", F.col("total_amount").cast("decimal(14,2)"))

        # Cast surrogate keys
        .withColumn("date_sk", F.col("date_sk").cast("int"))
        .withColumn("customer_sk", F.col("customer_sk").cast("bigint"))
        .withColumn("product_sk", F.col("product_sk").cast("bigint"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Final Fact Projection (Contract Enforcement)
df_fact_final = df_fact_aligned.select(
    "purchase_id",
    "customer_sk",
    "product_sk",
    "date_sk",
    "quantity",
    "unit_price",
    "total_amount",
    "batch_id"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Persist Fact Purchases (Gold)
(
    df_fact_final
        .write
        .format("delta")
        .mode("append")
        .saveAsTable("gold.fact_purchases")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
