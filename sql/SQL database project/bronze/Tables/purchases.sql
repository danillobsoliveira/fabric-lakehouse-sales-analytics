CREATE TABLE [bronze].[purchases] (

	[purchase_date] varchar(8000) NULL, 
	[purchase_id] bigint NULL, 
	[purchase_total] float NULL, 
	[ingestion_timestamp] datetime2(6) NULL, 
	[source_system] varchar(8000) NULL, 
	[batch_id] varchar(8000) NULL
);