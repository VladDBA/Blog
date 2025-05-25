SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE VIEW sys.dm_db_internal_auto_tuning_recommendation_impact_query_metrics
AS
SELECT [recommendation_id],
	[query_hash],
	[metric],
	[metric_name] = CASE [metric]
		-- the mapping here is not comprehensive and can be extended later
		WHEN 0 THEN N'EstimatedQoCost'
		WHEN 1 THEN N'ObservedExecutionCountPerHour'
		WHEN 2 THEN N'EstimatedCpuCostPerExecution'
		WHEN 3 THEN N'ObservedCpuCostPerExecution'
		ELSE N'Unknown'
		END,
	[avg_metric_before_action],
	[avg_metric_after_action]
FROM OPENROWSET(TABLE DM_DB_INTERNAL_ATS_INDEX_RECOMMENDATION_IMPACT_QUERIES)

