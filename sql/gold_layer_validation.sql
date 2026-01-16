-- ================================================
-- Gold Layer Validation
-- Purpose: Validate dimensional and fact tables
-- ================================================

-- 01 Preview dim_customer
SELECT TOP (100)
       customer_sk,
       customer_id,
       customer_name,
       customer_email,
       hash_diff,
       is_current,
       valid_from,
       valid_to,
       batch_id
FROM lh_sales_analytics.gold.dim_customer;

-- 02 Preview dim_date
SELECT TOP (100)
       date_sk,
       full_date,
       year,
       month,
       month_name,
       quarter,
       day,
       day_of_week,
       is_weekend
FROM lh_sales_analytics.gold.dim_date;

-- 03 Preview dim_product
SELECT TOP (100)
       product_sk,
       product_id,
       product_name,
       product_category,
       hash_diff,
       is_current,
       valid_from,
       valid_to,
       batch_id
FROM lh_sales_analytics.gold.dim_product;

-- 04 Preview fact_purchases
SELECT TOP (100)
       purchase_id,
       customer_sk,
       product_sk,
       date_sk,
       quantity,
       unit_price,
       total_amount,
       batch_id
FROM lh_sales_analytics.gold.fact_purchases;

-- 05 Count records in dimension tables
SELECT COUNT(1) AS dim_customer_count
FROM lh_sales_analytics.gold.dim_customer;

SELECT COUNT(1) AS dim_date_count
FROM lh_sales_analytics.gold.dim_date;

SELECT COUNT(1) AS dim_product_count
FROM lh_sales_analytics.gold.dim_product;

-- 06 Count records in fact table
SELECT COUNT(1) AS fact_purchases_count
FROM lh_sales_analytics.gold.fact_purchases;

-- 07 Check for nulls in key columns
SELECT COUNT(1) AS missing_customer_sk
FROM lh_sales_analytics.gold.dim_customer
WHERE customer_sk IS NULL;

SELECT COUNT(1) AS missing_product_sk
FROM lh_sales_analytics.gold.dim_product
WHERE product_sk IS NULL;

SELECT COUNT(1) AS missing_date_sk
FROM lh_sales_analytics.gold.dim_date
WHERE date_sk IS NULL;

SELECT COUNT(1) AS missing_fact_keys
FROM lh_sales_analytics.gold.fact_purchases
WHERE customer_sk IS NULL
   OR product_sk IS NULL
   OR date_sk IS NULL;

-- 08 Check for duplicates in surrogate keys
SELECT customer_sk, COUNT(1) AS duplicates
FROM lh_sales_analytics.gold.dim_customer
GROUP BY customer_sk
HAVING COUNT(1) > 1;

SELECT product_sk, COUNT(1) AS duplicates
FROM lh_sales_analytics.gold.dim_product
GROUP BY product_sk
HAVING COUNT(1) > 1;

SELECT date_sk, COUNT(1) AS duplicates
FROM lh_sales_analytics.gold.dim_date
GROUP BY date_sk
HAVING COUNT(1) > 1;

SELECT purchase_id, COUNT(1) AS duplicates
FROM lh_sales_analytics.gold.fact_purchases
GROUP BY purchase_id
HAVING COUNT(1) > 1;

-- 09 Check referential integrity: fact -> dimensions
SELECT f.purchase_id
FROM lh_sales_analytics.gold.fact_purchases f
LEFT JOIN lh_sales_analytics.gold.dim_customer c
       ON f.customer_sk = c.customer_sk
WHERE c.customer_sk IS NULL;

SELECT f.purchase_id
FROM lh_sales_analytics.gold.fact_purchases f
LEFT JOIN lh_sales_analytics.gold.dim_product p
       ON f.product_sk = p.product_sk
WHERE p.product_sk IS NULL;

SELECT f.purchase_id
FROM lh_sales_analytics.gold.fact_purchases f
LEFT JOIN lh_sales_analytics.gold.dim_date d
       ON f.date_sk = d.date_sk
WHERE d.date_sk IS NULL;

-- 10 Optional: simple consistency check in fact table
SELECT purchase_id,
       quantity,
       unit_price,
       total_amount,
       CASE WHEN total_amount <> quantity * unit_price THEN 1 ELSE 0 END AS mismatch_flag
FROM lh_sales_analytics.gold.fact_purchases
WHERE total_amount <> quantity * unit_price;