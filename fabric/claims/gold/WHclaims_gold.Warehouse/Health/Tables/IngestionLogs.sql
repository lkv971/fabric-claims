CREATE TABLE [Health].[IngestionLogs] (

	[IngestionID] uniqueidentifier NOT NULL, 
	[PipelineName] varchar(100) NOT NULL, 
	[Layer] varchar(20) NOT NULL, 
	[TargetObject] varchar(200) NULL, 
	[Status] varchar(30) NOT NULL, 
	[FinishedAtUTC] datetime2(3) NOT NULL, 
	[WatermarkBefore] varchar(40) NULL, 
	[WatermarkAfter] varchar(40) NULL, 
	[RowsWritten] bigint NULL, 
	[ErrorMessage] varchar(4000) NULL, 
	[RunID] varchar(100) NULL, 
	[BatchID] varchar(50) NULL, 
	[TriggerType] varchar(50) NULL
);


GO
ALTER TABLE [Health].[IngestionLogs] ADD CONSTRAINT PK_IngestionLogs primary key NONCLUSTERED ([IngestionID]);