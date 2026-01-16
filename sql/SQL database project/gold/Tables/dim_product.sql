CREATE TABLE [gold].[dim_product] (

	[product_sk] bigint NULL, 
	[product_id] bigint NULL, 
	[product_name] varchar(8000) NULL, 
	[product_category] varchar(8000) NULL, 
	[hash_diff] varchar(8000) NULL, 
	[is_current] bit NULL, 
	[valid_from] datetime2(6) NULL, 
	[valid_to] datetime2(6) NULL, 
	[batch_id] varchar(8000) NULL
);