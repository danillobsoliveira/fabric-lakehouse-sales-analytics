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
# META     },
# META     "environment": {
# META       "environmentId": "df8fac0b-9074-99ff-45d3-3861991da1b5",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 🥈 Silver Layer — Data Transformation
# 
# **Objective**
# 
# This notebook is responsible for transforming Bronze layer data into a clean,
# normalized, and business-ready format. It applies data cleaning, standardization,
# and enrichment rules to prepare datasets for analytical modeling.
# 
# **Execution Context**
# - **Platform:** Microsoft Fabric
# - **Compute:** Spark (Lakehouse attached)
# - **Storage format:** Delta Lake
# - **Execution mode:** Batch
# - **Orchestration:** Fabric Data Pipeline

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
from pyspark.sql.window import Window
from pyspark.sql.functions import col, explode, to_date, year, month

# ============================================================
# Execution Context
# ============================================================

ENV = "dev"
LAYER = "silver"
PIPELINE_NAME = "silver_transformation"

# ============================================================
# Orchestration Context
# ============================================================

batch_id = spark.conf.get("pipeline.batchId", "manual_run")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Bronze Tables
# 
# This section loads curated datasets from the Bronze layer.
# The Silver layer never reads raw files directly, ensuring proper
# data lineage, governance, and reprocessing capabilities.

# CELL ********************

# Load data from the Bronze layer table.
bronze_df = spark.read.table("bronze.purchases")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Scope of Transformations
# 
# The following transformations are applied in this layer:
# 
# - **Data Normalization:** Normalization of nested structures
# - **Data Cleaning:** Removal of duplicated and invalid records
# - **Data Standardization:** Standardization of data types and formats
# - **Derived Columns:** Derivation of analytical columns
# - **Data Enrichment:** Enrichment with reference data

# MARKDOWN ********************

# ### Normalization of Nested Structures
# 
# Flatten nested arrays and structs to ensure consistent row-level granularity.
# - Explode the 'items' array to have one row per item.
# - Flatten the 'customer' struct into individual columns.
# - This prepares the dataset for cleaning, standardization, and analytical derivations.

# CELL ********************

# ------------------------------------------------------------
# Step 1: Explode 'items' array
# ------------------------------------------------------------
# The 'items' column is an ARRAY<STRUCT>.
# Exploding this array creates one row per item,
# which is the required granularity for analytical
# and dimensional modeling (fact tables).
# ------------------------------------------------------------

df_exploded = (
    bronze_df
    .withColumn("item", explode(col("items")))
)

# ------------------------------------------------------------
# Step 2: Flatten nested STRUCT fields
# ------------------------------------------------------------
# Both 'customer' and 'item' are STRUCT types.
# This step extracts their attributes into
# individual, well-named columns.

df_silver_sales_items = (
    df_exploded
    .select(
        
        # Purchase-level attributes
        col("purchase_id"),
        col("purchase_date"),
        col("purchase_total"),

         # Customer attributes
        col("customer.customer_id").alias("customer_id"),
        col("customer.email").alias("customer_email"),
        col("customer.name").alias("customer_name"),

        # Item / Product attributes
        col("item.product_id").alias("product_id"),
        col("item.product_name").alias("product_name"),
        col("item.category").alias("product_category"),
        col("item.quantity").alias("quantity"),
        col("item.unit_price").alias("unit_price"),
        col("item.item_total").alias("item_total_value"),

        # Technical metadata
        col("ingestion_timestamp"),
        col("source_system"),
        col("batch_id")
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Data Cleaning
# 
# Remove duplicated and invalid records to ensure data quality.
# - Remove duplicated items at purchase level.
# - Filter out records with invalid quantities or monetary values.

# CELL ********************

# Data Cleaning

df_cleaned = (
    df_silver_sales_items
    .dropDuplicates(["purchase_id", "product_id"])
    .filter(col("quantity") > 0)
    .filter(col("unit_price") >= 0)
    .filter(col("item_total_value") > 0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Data Standardization
# 
# Standardize data types and formats for analytical consistency.
# - Cast date and numeric columns to appropriate types.
# - Ensure consistent precision for monetary values.

# CELL ********************

# Data Standardization

df_standardized = (
    df_cleaned
    .withColumn("purchase_date", to_date(col("purchase_date")))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("decimal(12,2)"))
    .withColumn("item_total_value", col("item_total_value").cast("decimal(14,2)"))
    .withColumn("purchase_total", col("purchase_total").cast("decimal(14,2)"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Derived Columns
# 
# Derive analytical columns to support reporting and validation.
# - Create time-based attributes.
# - Add calculated metrics for consistency checks.

# CELL ********************

# Derived Columns

df_derived = (
    df_standardized
    .withColumn("purchase_year", F.year(col("purchase_date")))
    .withColumn("purchase_month", F.month(col("purchase_date")))
    .withColumn(
        "calculated_item_total",
        col("quantity") * col("unit_price")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Data Enrichment
# 
# Enrich the dataset with reference data to add business context.

# CELL ********************

# No enrichment applied in this layer
df_silver_enriched = df_derived

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Quality Validation
# 
# This section validates the **semantic integrity, data quality, and modeling rules**
# applied during the Silver transformation phase.

# CELL ********************

# 1. Structural validation
if df_silver_enriched.count() == 0:
    raise ValueError("No records to persist in Silver layer")

# 2. Key integrity validation
if df_silver_enriched.filter(col("purchase_id").isNull()).count() > 0:
    raise ValueError("Null values found in purchase_id")

# 3. Business sanity checks
if df_silver_enriched.filter(col("quantity") <= 0).count() > 0:
    raise ValueError("Invalid quantity values detected")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Persistence
# 
# Persist the validated Silver dataset for downstream consumption.
# - Write data in Delta format.
# - Use managed tables for governance and discoverability.
# - Overwrite mode applied for full Silver refresh.

# CELL ********************

# Persist Silver dataset as a managed Delta table
(
    df_silver_enriched
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("silver.purchases")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
