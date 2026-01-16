CREATE TABLE [gold].[fact_purchases] (

	[purchase_id] bigint NULL, 
	[customer_sk] bigint NULL, 
	[product_sk] bigint NULL, 
	[date_sk] int NULL, 
	[quantity] int NULL, 
	[unit_price] decimal(12,2) NULL, 
	[total_amount] decimal(14,2) NULL, 
	[batch_id] varchar(8000) NULL
);