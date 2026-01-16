CREATE TABLE [gold].[dim_date] (

	[date_sk] int NULL, 
	[full_date] date NULL, 
	[year] int NULL, 
	[month] int NULL, 
	[month_name] varchar(8000) NULL, 
	[quarter] int NULL, 
	[day] int NULL, 
	[day_of_week] varchar(8000) NULL, 
	[is_weekend] bit NULL
);