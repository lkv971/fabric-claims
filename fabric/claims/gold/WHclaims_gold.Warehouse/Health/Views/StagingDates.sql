-- Auto Generated (Do not modify) 59CEECABE8562FF842B118CADC3B12B55A7239144D071C73439DFE67DAA0A608
CREATE VIEW Health.StagingDates 
AS SELECT DISTINCT
DateID,
Date,
Year,
Month,
Day,
MonthName,
DayName
FROM LHclaims_silver.dbo.dim_dates
;