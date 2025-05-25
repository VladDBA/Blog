use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE VIEW sys.dm_db_internal_auto_tuning_recommendation_metrics
AS
SELECT [recommendation_id],
	[dimension_id],
	[dimension_name] = CASE [dimension_id]
		-- the mapping here is not comprehensive and can be extended later
		WHEN 0 THEN N'SpaceChange'
		WHEN 1 THEN N'QueriesWithRegressedPerformance'
		WHEN 2 THEN N'CpuUtilization'
		WHEN 3 THEN N'LogicalReads'
		WHEN 4 THEN N'LogicalWrites'
		WHEN 5 THEN N'AffectedQueriesCpuUtilization'
		WHEN 6 THEN N'AffectedQueriesLogicalReads'
		WHEN 7 THEN N'AffectedQueriesLogicalWrites'
		WHEN 8 THEN N'VerificationProgress'
		WHEN 9 THEN N'QueriesWithImprovedPerformance'
		ELSE N'Unknown'
		END,
	[impact_type_id],
	[impact_type_name] = CASE [impact_type_id]
		-- the mapping here is not comprehensive and can be extended later
		WHEN 0 THEN N'Estimated'
		WHEN 1 THEN N'Observed'
		ELSE N'Unknown'
		END,
	[unit_type_id],
	[unit_type_name] = CASE [unit_type_id]
		-- the mapping here is not comprehensive and can be extended later
		WHEN 0 THEN N'MegaBytes'
		WHEN 1 THEN N'CpuCores'
		WHEN 2 THEN N'ReadsPerSecond'
		WHEN 3 THEN N'WritesPerSecond'
		WHEN 4 THEN N'Percent'
		WHEN 5 THEN N'Count'
		ELSE N'Unknown'
		END,
	[absolute_value],
	[change_value_absolute],
	[change_value_relative]
FROM OPENROWSET(TABLE DM_DB_INTERNAL_ATS_RECOMMENDED_IMPACT_VALUES)

