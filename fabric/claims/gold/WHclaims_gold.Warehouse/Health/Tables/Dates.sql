CREATE TABLE [Health].[Dates] (

	[DateID] int NOT NULL, 
	[Date] date NULL, 
	[Year] int NULL, 
	[Month] int NULL, 
	[Day] int NULL, 
	[MonthName] varchar(50) NULL, 
	[DayName] varchar(50) NULL
);


GO
ALTER TABLE [Health].[Dates] ADD CONSTRAINT PK_Dates primary key NONCLUSTERED ([DateID]);