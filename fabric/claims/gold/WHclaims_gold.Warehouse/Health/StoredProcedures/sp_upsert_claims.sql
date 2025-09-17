CREATE   PROCEDURE Health.sp_upsert_claims
AS 
BEGIN
    SET NOCOUNT ON;

    DECLARE
    @uProv INT = 0, @iProv INT = 0,
    @uPat  INT = 0, @iPat  INT = 0,
    @iDates INT = 0,
    @uClaim INT = 0, @iClaim INT = 0;

    UPDATE HPV
    SET 
        HPV.ProviderSpecialty = SHPV.ProviderSpecialty,
        HPV.ProviderLocation = SHPV.ProviderLocation
    FROM Health.Providers AS HPV
    INNER JOIN Health.StagingProviders AS SHPV
        ON HPV.ProviderID = SHPV.ProviderID
    WHERE ISNULL(HPV.ProviderSpecialty, '') <> ISNULL(SHPV.ProviderSpecialty, '')
        OR ISNULL(HPV.ProviderLocation, '') <> ISNULL(SHPV.ProviderLocation, '')
    ;
    SET @uProv = @@ROWCOUNT;

    INSERT INTO Health.Providers (ProviderID, ProviderSpecialty, ProviderLocation)
    SELECT SHPV.ProviderID, SHPV.ProviderSpecialty, SHPV.ProviderLocation
    FROM Health.StagingProviders AS SHPV
    WHERE NOT EXISTS (
        SELECT 1 
        FROM Health.Providers AS HPV
        WHERE HPV.ProviderID = SHPV.ProviderID
    );
    SET @iProv = @@ROWCOUNT;

    UPDATE HPT
    SET
        HPT.PatientAge = SHPT.PatientAge,
        HPT.PatientGender = SHPT.PatientGender,
        HPT.PatientMaritalStatus = SHPT.PatientMaritalStatus,
        HPT.PatientEmploymentStatus = SHPT.PatientEmploymentStatus
    FROM Health.Patients AS HPT
    INNER JOIN Health.StagingPatients AS SHPT
        ON HPT.PatientID = SHPT.PatientID 
    WHERE ISNULL(HPT.PatientAge, -1) <> ISNULL(SHPT.PatientAge, -1)
        OR ISNULL(HPT.PatientGender, '') <> ISNULL(SHPT.PatientGender, '')
        OR ISNULL(HPT.PatientMaritalStatus, '') <> ISNULL(SHPT.PatientMaritalStatus, '')
        OR ISNULL(HPT.PatientEmploymentStatus, '') <> ISNULL(SHPT.PatientEmploymentStatus, '')
    ;
    SET @uPat = @@ROWCOUNT;

    INSERT INTO Health.Patients (PatientID, PatientAge, PatientGender, PatientMaritalStatus, PatientEmploymentStatus)
    SELECT SHPT.PatientID, SHPT.PatientAge, SHPT.PatientGender, SHPT.PatientMaritalStatus, SHPT.PatientEmploymentStatus
    FROM Health.StagingPatients AS SHPT
    WHERE NOT EXISTS (
        SELECT 1 
        FROM Health.Patients AS HPT 
        WHERE HPT.PatientID = SHPT.PatientID
    );
    SET @iPat = @@ROWCOUNT;

    INSERT INTO Health.Dates (DateID, [Date], [Year], [Month], [Day], MonthName, DayName)
    SELECT HSD.DateID, HSD.[Date], HSD.[Year], HSD.[Month], HSD.[Day], HSD.MonthName, HSD.DayName
    FROM Health.StagingDates AS HSD
    WHERE NOT EXISTS (
        SELECT 1
        FROM Health.Dates AS HD
        WHERE HD.DateID = HSD.DateID
    );
    SET @iDates = @@ROWCOUNT;
    UPDATE HC 
    SET
        HC.PatientID = HSC.PatientID,
        HC.ProviderID = HSC.ProviderID,
        HC.ClaimDate = HSC.ClaimDate,
        HC.ClaimAmount = HSC.ClaimAmount,
        HC.DiagnosisCode = HSC.DiagnosisCode,
        HC.ProcedureCode = HSC.ProcedureCode,
        HC.ClaimStatus = HSC.ClaimStatus,
        HC.ClaimType = HSC.ClaimType,
        HC.ClaimSubmissionMethod = HSC.ClaimSubmissionMethod
    FROM Health.Claims AS HC
    INNER JOIN Health.StagingClaims AS HSC
        ON HC.ClaimID = HSC.ClaimID
    WHERE ISNULL(HC.PatientID, '') <> ISNULL(HSC.PatientID, '')
        OR ISNULL(HC.ProviderID, '') <> ISNULL(HSC.ProviderID, '')
        OR ISNULL(HC.ClaimDate, '1900-01-01') <> ISNULL(HSC.ClaimDate, '1900-01-01')
        OR ISNULL(HC.ClaimAmount, 0) <> ISNULL(HSC.ClaimAmount, 0)
        OR ISNULL(HC.DiagnosisCode, '') <> ISNULL(HSC.DiagnosisCode, '')
        OR ISNULL(HC.ProcedureCode, '') <> ISNULL(HSC.ProcedureCode, '')
        OR ISNULL(HC.ClaimStatus, '') <> ISNULL(HSC.ClaimStatus, '')
        OR ISNULL(HC.ClaimType, '') <> ISNULL(HSC.ClaimType, '')
        OR ISNULL(HC.ClaimSubmissionMethod, '') <> ISNULL(HSC.ClaimSubmissionMethod, '')
    ;
    SET @uClaim = @@ROWCOUNT;

    INSERT INTO Health.Claims (ClaimID, PatientID, ProviderID, ClaimDate, ClaimAmount, DiagnosisCode, ProcedureCode, ClaimStatus, ClaimType, ClaimSubmissionMethod)
    SELECT HSC.ClaimID, HSC.PatientID, HSC.ProviderID, HSC.ClaimDate, HSC.ClaimAmount, HSC.DiagnosisCode, HSC.ProcedureCode, HSC.ClaimStatus, HSC.ClaimType, HSC.ClaimSubmissionMethod
    FROM Health.StagingClaims AS HSC
    WHERE NOT EXISTS (
        SELECT 1
        FROM Health.Claims AS HC
        WHERE HC.ClaimID = HSC.ClaimID
    );
    SET @iClaim = @@ROWCOUNT;

    SELECT
        RowsUpdated_Providers  = @uProv,
        RowsInserted_Providers = @iProv,
        RowsUpdated_Patients   = @uPat,
        RowsInserted_Patients  = @iPat,
        RowsInserted_Dates     = @iDates,
        RowsUpdated_Claims     = @uClaim,
        RowsInserted_Claims    = @iClaim,
        TotalRowsWritten       = (@uProv + @iProv + @uPat + @iPat + @iDates + @uClaim + @iClaim);

END;