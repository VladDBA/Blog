SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.sysquery_store_runtime_stats_2016 AS
	SELECT
		runtime_stats_id,
		plan_id,
		runtime_stats_interval_id,
		execution_type,
		n.name as execution_type_desc,
		first_execution_time,
		last_execution_time,
		count_executions,
		avg_duration,
		last_duration,
		min_duration,
		max_duration,
		CASE WHEN sqdiff_duration >= 0 THEN sqrt(sqdiff_duration) ELSE NULL END as stdev_duration,
		avg_cpu_time,
		last_cpu_time,
		min_cpu_time,
		max_cpu_time,
		CASE WHEN sqdiff_cpu_time >= 0 THEN sqrt(sqdiff_cpu_time) ELSE NULL END as stdev_cpu_time,
		avg_logical_io_reads,
		last_logical_io_reads,
		min_logical_io_reads,
		max_logical_io_reads,
		CASE WHEN sqdiff_logical_io_reads >= 0 THEN sqrt(sqdiff_logical_io_reads) ELSE NULL END as stdev_logical_io_reads,
		avg_logical_io_writes,
		last_logical_io_writes,
		min_logical_io_writes,
		max_logical_io_writes,
		CASE WHEN sqdiff_logical_io_writes >= 0 THEN sqrt(sqdiff_logical_io_writes) ELSE NULL END as stdev_logical_io_writes,
		avg_physical_io_reads,
		last_physical_io_reads,
		min_physical_io_reads,
		max_physical_io_reads,
		CASE WHEN sqdiff_physical_io_reads >= 0 THEN sqrt(sqdiff_physical_io_reads) ELSE NULL END as stdev_physical_io_reads,
		avg_clr_time,
		last_clr_time,
		min_clr_time,
		max_clr_time,
		CASE WHEN sqdiff_clr_time >= 0 THEN sqrt(sqdiff_clr_time) ELSE NULL END as stdev_clr_time,
		avg_dop,
		last_dop,
		min_dop,
		max_dop,
		CASE WHEN sqdiff_dop >= 0 THEN sqrt(sqdiff_dop) ELSE NULL END as stdev_dop,
		avg_query_max_used_memory,
		last_query_max_used_memory,
		min_query_max_used_memory,
		max_query_max_used_memory,
		CASE WHEN sqdiff_query_max_used_memory >= 0 THEN sqrt(sqdiff_query_max_used_memory) ELSE NULL END as stdev_query_max_used_memory,
		avg_rowcount,
		last_rowcount,
		min_rowcount,
		max_rowcount,
		CASE WHEN sqdiff_rowcount >= 0 THEN sqrt(sqdiff_rowcount) ELSE NULL END as stdev_rowcount
	FROM
	(
		SELECT
		*,
		round( convert(float, sumsquare_duration) / count_executions - avg_duration * avg_duration,2) as sqdiff_duration,
		round( convert(float, sumsquare_cpu_time) / count_executions  - avg_cpu_time * avg_cpu_time,2) as sqdiff_cpu_time,
		round( convert(float, sumsquare_logical_io_reads) / count_executions - avg_logical_io_reads * avg_logical_io_reads,2) as sqdiff_logical_io_reads,
		round( convert(float, sumsquare_logical_io_writes) / count_executions - avg_logical_io_writes * avg_logical_io_writes,2) as sqdiff_logical_io_writes,
		round( convert(float, sumsquare_physical_io_reads) / count_executions - avg_physical_io_reads * avg_physical_io_reads,2) as sqdiff_physical_io_reads,
		round( convert(float, sumsquare_clr_time) / count_executions - avg_clr_time * avg_clr_time,2) as sqdiff_clr_time,
		round( convert(float, sumsquare_dop) / count_executions - avg_dop * avg_dop,2) as sqdiff_dop,
		round( convert(float, sumsquare_query_max_used_memory) / count_executions - avg_query_max_used_memory * avg_query_max_used_memory,2) as sqdiff_query_max_used_memory,
		round( convert(float, sumsquare_rowcount) / count_executions - avg_rowcount * avg_rowcount,2) as sqdiff_rowcount
		FROM
		(
			SELECT 
			rs.*,
			CONVERT(float, total_duration) / count_executions AS avg_duration,
			CONVERT(float, total_cpu_time) / count_executions AS avg_cpu_time,
			CONVERT(float, total_logical_io_reads) / count_executions AS avg_logical_io_reads,
			CONVERT(float, total_logical_io_writes) / count_executions AS avg_logical_io_writes,
			CONVERT(float, total_physical_io_reads) / count_executions AS avg_physical_io_reads,
			CONVERT(float, total_clr_time) / count_executions AS avg_clr_time,
			CONVERT(float, total_dop) / count_executions AS avg_dop,
			CONVERT(float, total_query_max_used_memory) / count_executions AS avg_query_max_used_memory,
			CONVERT(float, total_rowcount) / count_executions AS avg_rowcount
			FROM sys.sysplan_persist_runtime_stats_merged_2016 rs
		) AS AVG_V
	) AS AVG_SQ_V
	LEFT JOIN sys.syspalvalues n ON n.class = 'QDXT' and n.value = execution_type

