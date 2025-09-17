-- Auto Generated (Do not modify) 46CDBEE9302DC7FB3B76F88E6C1BE30C8AFF568A7E8816A60171814C23B6A89A
CREATE VIEW Health.StagingPatients
AS SELECT DISTINCT
PatientID,
PatientAge,
PatientGender,
PatientMaritalStatus,
PatientEmploymentStatus
FROM LHclaims_silver.dbo.dim_patients
;