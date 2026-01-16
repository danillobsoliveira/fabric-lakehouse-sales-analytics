/* ================================================
   Bronze Layer Validation
   Purpose: Validate raw ingestion layer
================================================ */

/* 01 Sample data */
SELECT TOP (100)
       purchase_date,
       purchase_id,
       purchase_total,
       ingestion_timestamp,
       source_system,
       batch_id
FROM lh_sales_analytics.bronze.purchases;

/* 02 Count records by batch */
SELECT batch_id,
       COUNT(1) AS records_count
FROM lh_sales_analytics.bronze.purchases
GROUP BY batch_id
ORDER BY batch_id;

/* 03 Check for nulls in key columns */
SELECT COUNT(1) AS missing_keys
FROM lh_sales_analytics.bronze.purchases
WHERE purchase_id IS NULL;

/* 04 Check for duplicate keys */
SELECT purchase_id,
       COUNT(1) AS duplicates
FROM lh_sales_analytics.bronze.purchases
GROUP BY purchase_id
HAVING COUNT(1) > 1;