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

# # 📦 Synthetic Commerce Dataset Generator
# 
# **Objective**
# 
# This notebook generates a **synthetic commerce dataset** designed to simulate realistic
# transactional behavior.
# 
# **Execution Context**
# - **Platform:** Microsoft Fabric  
# - **Compute:** Python Notebook  
# - **Storage:** OneLake (Lakehouse Files)  
# - **Data Format:** JSON  
# - **Execution Mode:** Batch  
# - **Downstream Usage:** Bronze Layer ingestion
# 
# > This notebook generates synthetic datasets exclusively for Bronze layer ingestion
# > and architectural demonstration purposes. It is not intended for production data generation.

# MARKDOWN ********************

# ## Runtime Configuration & Dependencies
# 
# This section defines execution parameters and required libraries.

# CELL ********************

# ============================================================
# Library Imports
# ============================================================

import json
import random
import math
from faker import Faker
from datetime import datetime, timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# ============================================================
# Runtime Configuration
# ============================================================

# Total number of purchase transactions to generate
DATASET_SIZE = 1_000

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Automatic Dataset Derivation
# 
# This section derives master data sizes automatically based on the dataset scale.
# No hardcoded dimensions are required.

# CELL ********************

# ============================================================
# Automatic Dataset Derivation
# ============================================================

fake = Faker("en_US")

# Derive master data size automatically
NUM_CUSTOMERS = max(5, int(math.sqrt(DATASET_SIZE)))
NUM_PRODUCTS = max(5, int(math.sqrt(DATASET_SIZE)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Domain Reference Data
# 
# Defines domain-level reference data used across the dataset.

# CELL ********************

# ============================================================
# Product Categories
# ============================================================

PRODUCT_CATEGORIES = {
    "Electronics": ["Notebook", "Smartphone", "Tablet", "Monitor"],
    "Home": ["Chair", "Table", "Lamp", "Sofa"],
    "Books": ["Fiction", "Science", "Education"],
    "Clothing": ["T-Shirt", "Jacket", "Jeans"]
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Customer Master Data Generation
# 
# Generates synthetic customer entities.

# CELL ********************

# ============================================================
# Customer Master Data
# ============================================================

def generate_customers(n: int) -> list:
    return [
        {
            "customer_id": i + 1,
            "name": fake.name(),
            "email": fake.email()
        }
        for i in range(n)
    ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Product Master Data Generation
# 
# Generates a synthetic product catalog with categories and pricing.

# CELL ********************

# ============================================================
# Product Master Data
# ============================================================

def generate_products(n: int) -> list:

    products = []
    product_id = 1
    category_list = list(PRODUCT_CATEGORIES.items())

    while len(products) < n:
        category, names = random.choice(category_list)
        name = random.choice(names)

        products.append({
            "product_id": product_id,
            "product_name": f"{name} {fake.word().capitalize()}",
            "category": category,
            "unit_price": round(random.uniform(50, 5000), 2)
        })

        product_id += 1

    return products

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transactional Data Generation
# 
# This section generates purchase transactions.
# Each transaction may contain multiple distinct products,
# each with independent quantities and calculated totals.

# CELL ********************

# ============================================================
# Transactional Data Generation
# ============================================================

def generate_purchases(
    num_purchases: int,
    customers: list,
    products: list
) -> list:

    purchases = []

    for purchase_id in range(1, num_purchases + 1):

        customer = random.choice(customers)

        purchase_date = (
            datetime.now() - timedelta(days=random.randint(0, 120))
        ).isoformat()

        # Number of distinct products per purchase
        num_items = random.randint(1, min(5, len(products)))
        selected_products = random.sample(products, k=num_items)

        items = []
        purchase_total = 0

        for product in selected_products:
            quantity = random.randint(1, 5)
            item_total = quantity * product["unit_price"]
            purchase_total += item_total

            items.append({
                "product_id": product["product_id"],
                "product_name": product["product_name"],
                "category": product["category"],
                "quantity": quantity,
                "unit_price": product["unit_price"],
                "item_total": round(item_total, 2)
            })

        purchases.append({
            "purchase_id": purchase_id,
            "purchase_date": purchase_date,
            "customer": customer,
            "items": items,
            "purchase_total": round(purchase_total, 2)
        })

    return purchases

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dataset Assembly
# 
# Assembles the full dataset structure, including transactional data and metadata.

# CELL ********************

# ============================================================
# Dataset Assembly
# ============================================================

def generate_dataset(quantity: int) -> dict:

    customers = generate_customers(NUM_CUSTOMERS)
    products = generate_products(NUM_PRODUCTS)
    purchases = generate_purchases(quantity, customers, products)

    return {
        "data": purchases,
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "record_count": quantity,
            "derived_customers": len(customers),
            "derived_products": len(products),
            "source_system": "synthetic_dataset_generator"
        }
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dataset Generation & Validation
# 
# Generates the dataset and validates basic metadata.

# CELL ********************

# ============================================================
# Dataset Generation
# ============================================================

dataset = generate_dataset(DATASET_SIZE)

dataset["metadata"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Persisting Dataset to Lakehouse Files
# 
# Persists the dataset as a JSON snapshot in OneLake.
# The file is intentionally overwritten to keep the dataset idempotent.

# CELL ********************

# ============================================================
# Persisting Dataset to Lakehouse Files
# ============================================================

output_path = "/lakehouse/default/Files/purchases.json"

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(dataset, f, indent=2)

output_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
