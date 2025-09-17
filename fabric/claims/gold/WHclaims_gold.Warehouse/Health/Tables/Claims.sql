CREATE TABLE [Health].[Claims] (

	[ClaimID] varchar(100) NOT NULL, 
	[PatientID] varchar(100) NULL, 
	[ProviderID] varchar(100) NULL, 
	[ClaimDate] date NULL, 
	[ClaimAmount] float NULL, 
	[DiagnosisCode] varchar(50) NULL, 
	[ProcedureCode] varchar(50) NULL, 
	[ClaimStatus] varchar(50) NULL, 
	[ClaimType] varchar(50) NULL, 
	[ClaimSubmissionMethod] varchar(50) NULL
);


GO
ALTER TABLE [Health].[Claims] ADD CONSTRAINT PK_Claims primary key NONCLUSTERED ([ClaimID]);
GO
ALTER TABLE [Health].[Claims] ADD CONSTRAINT FK_ClaimsPatientID FOREIGN KEY ([PatientID]) REFERENCES [Health].[Patients]([PatientID]);
GO
ALTER TABLE [Health].[Claims] ADD CONSTRAINT FK_ClaimsProviderID FOREIGN KEY ([ProviderID]) REFERENCES [Health].[Providers]([ProviderID]);