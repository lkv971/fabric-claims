-- Auto Generated (Do not modify) C67F628DE8A3398E8358E0F3EA865F4AFC64054ED95DA320210A7BBDAB00C8E8
CREATE VIEW  Health.StagingProviders
AS SELECT DISTINCT
ProviderID,
ProviderSpecialty,
ProviderLocation
FROM LHclaims_silver.dbo.dim_providers
;