CREATE TABLE [gold].[dim_customer] (

	[customer_sk] bigint NULL, 
	[customer_id] bigint NULL, 
	[customer_name] varchar(8000) NULL, 
	[customer_email] varchar(8000) NULL, 
	[hash_diff] varchar(8000) NULL, 
	[is_current] bit NULL, 
	[valid_from] datetime2(6) NULL, 
	[valid_to] datetime2(6) NULL, 
	[batch_id] varchar(8000) NULL
);