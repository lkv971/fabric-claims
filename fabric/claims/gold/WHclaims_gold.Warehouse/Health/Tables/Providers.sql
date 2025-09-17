CREATE TABLE [Health].[Providers] (

	[ProviderID] varchar(100) NOT NULL, 
	[ProviderSpecialty] varchar(100) NULL, 
	[ProviderLocation] varchar(100) NULL
);


GO
ALTER TABLE [Health].[Providers] ADD CONSTRAINT PK_Providers primary key NONCLUSTERED ([ProviderID]);