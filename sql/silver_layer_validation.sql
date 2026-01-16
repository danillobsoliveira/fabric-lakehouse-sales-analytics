/* ================================================
   Silver Layer Validation
   Purpose: Validate cleaned/enriched layer
================================================ */

-- 01 Sample data: preview 100 rows
SELECT TOP (100)
       purchase_id,
       purchase_date,
       purchase_total,
       customer_id,
       customer_email,
       customer_name,
       product_id,
       product_name,
       product_category,
       quantity,
       unit_price,
       item_total_value,
       ingestion_timestamp,
       source_system,
       batch_id,
       purchase_year,
       purchase_month,
       calculated_item_total
FROM lh_sales_analytics.silver.purchases;

-- 02 Count total records
SELECT COUNT(1) AS total_records
FROM lh_sales_analytics.silver.purchases;

-- 03 Count records by batch
SELECT batch_id,
       COUNT(1) AS records_count
FROM lh_sales_analytics.silver.purchases
GROUP BY batch_id
ORDER BY batch_id;

-- 04 Count records by product
SELECT product_id,
       COUNT(1) AS sales_count
FROM lh_sales_analytics.silver.purchases
GROUP BY product_id
ORDER BY sales_count DESC;

-- 05 Count records by customer
SELECT customer_id,
       COUNT(1) AS orders_count
FROM lh_sales_analytics.silver.purchases
GROUP BY customer_id
ORDER BY orders_count DESC;

-- 06 Check for nulls in key columns
SELECT COUNT(1) AS missing_purchase_id
FROM lh_sales_analytics.silver.purchases
WHERE purchase_id IS NULL;

SELECT COUNT(1) AS missing_customer_id
FROM lh_sales_analytics.silver.purchases
WHERE customer_id IS NULL;

SELECT COUNT(1) AS missing_product_id
FROM lh_sales_analytics.silver.purchases
WHERE product_id IS NULL;

-- 07 Check for duplicate purchase_id
SELECT purchase_id,
       COUNT(1) AS duplicates
FROM lh_sales_analytics.silver.purchases
GROUP BY purchase_id
HAVING COUNT(1) > 1;

-- 08 Check item_total_value consistency
SELECT purchase_id,
       quantity,
       unit_price,
       item_total_value,
       quantity * unit_price AS calculated_total,
       CASE WHEN item_total_value != quantity * unit_price THEN 1 ELSE 0 END AS mismatch_flag
FROM lh_sales_analytics.silver.purchases
WHERE item_total_value != quantity * unit_price;
