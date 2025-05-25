use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.sysquery_store_wait_stats_2019 AS
	SELECT
		wait_stats_id,
		plan_id,
		runtime_stats_interval_id,
		wait_category,
		c.name as wait_category_desc,
		execution_type,
		n.name as execution_type_desc,
		total_query_wait_time_ms,
		avg_query_wait_time_ms,
		last_query_wait_time_ms,
		min_query_wait_time_ms,
		max_query_wait_time_ms,
		CASE WHEN sqdiff_query_wait_time_ms >= 0 THEN sqrt(sqdiff_query_wait_time_ms) ELSE NULL END as stdev_query_wait_time_ms
	FROM
	(
		SELECT
		*,
		round( convert(float, sumsquare_query_wait_time_ms) / count_executions - avg_query_wait_time_ms * avg_query_wait_time_ms,2) as sqdiff_query_wait_time_ms
		FROM
		(
			SELECT 
			ws.*,
			CONVERT(float, total_query_wait_time_ms) / count_executions AS avg_query_wait_time_ms
			FROM sys.sysplan_persist_wait_stats_merged_2019 ws
		) AS AVG_VAL
	) AS AVG_SQ_VAL
	LEFT JOIN sys.syspalvalues c ON c.class = 'WCAT' and c.value = wait_category
	LEFT JOIN sys.syspalvalues n ON n.class = 'QDXT' and n.value = execution_type

