-- Auto Generated (Do not modify) 9B57E7A9C18F36870BA72DF20E19E96E83FF9193CC80795FBD4E109AC1ABB30B
CREATE VIEW Health.StagingClaims
AS SELECT DISTINCT
ClaimID,
PatientID,
ProviderID,
ClaimDate,
ClaimAmount,
DiagnosisCode,
ProcedureCode,
ClaimStatus,
ClaimType,
ClaimSubmissionMethod
FROM LHclaims_silver.dbo.fact_claims
;