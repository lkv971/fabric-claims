CREATE TABLE [Health].[Patients] (

	[PatientID] varchar(100) NOT NULL, 
	[PatientAge] int NULL, 
	[PatientGender] varchar(10) NULL, 
	[PatientMaritalStatus] varchar(20) NULL, 
	[PatientEmploymentStatus] varchar(30) NULL
);


GO
ALTER TABLE [Health].[Patients] ADD CONSTRAINT PK_Patients primary key NONCLUSTERED ([PatientID]);