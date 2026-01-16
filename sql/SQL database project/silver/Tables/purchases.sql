CREATE TABLE [silver].[purchases] (

	[purchase_id] bigint NULL, 
	[purchase_date] date NULL, 
	[purchase_total] decimal(14,2) NULL, 
	[customer_id] bigint NULL, 
	[customer_email] varchar(8000) NULL, 
	[customer_name] varchar(8000) NULL, 
	[product_id] bigint NULL, 
	[product_name] varchar(8000) NULL, 
	[product_category] varchar(8000) NULL, 
	[quantity] int NULL, 
	[unit_price] decimal(12,2) NULL, 
	[item_total_value] decimal(14,2) NULL, 
	[ingestion_timestamp] datetime2(6) NULL, 
	[source_system] varchar(8000) NULL, 
	[batch_id] varchar(8000) NULL, 
	[purchase_year] int NULL, 
	[purchase_month] int NULL, 
	[calculated_item_total] decimal(23,2) NULL
);