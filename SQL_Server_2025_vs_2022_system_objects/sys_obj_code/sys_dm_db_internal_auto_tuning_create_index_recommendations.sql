SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE VIEW sys.dm_db_internal_auto_tuning_create_index_recommendations
AS
SELECT r.[id], r.[state], sn.[name] AS [state_name], r.[type_id], r.[recommendation_timestamp_utc],
	r.[last_refresh_timestamp_utc], r.[archived], r.[extended_properties],
	ir.[index_type], ir.[schema], ir.[table], ir.[index_columns], ir.[included_columns],
	ir.[index_name], ir.[estimated_space_change_cached], ir.[estimated_action_duration_cached],
	ir.[reported_number_of_queries_with_improved_performance_cached],
	ir.[reported_number_of_queries_with_regressed_performance_cached],
	ir.[reported_cpu_utilization_change_absolute_cached],
	ir.[reported_cpu_utilization_change_relative_cached],
	ir.[reported_logical_reads_change_absolute_cached],
	ir.[reported_logical_reads_change_relative_cached],
	ir.[reported_logical_writes_change_absolute_cached],
	ir.[reported_logical_writes_change_relative_cached],
	ir.[index_size_kb_before_action],
	ir.[index_size_kb_after_action],
	ir.[recommendation_source],
	ir.[baseline_data_collecting_start_time_utc],
	ir.[score],
	r.[group_id]
FROM OPENROWSET(TABLE DM_DB_INTERNAL_ATS_RECOMMENDATIONS) AS r,
	OPENROWSET(TABLE DM_DB_INTERNAL_ATS_INDEX_RECOMMENDATIONS) AS ir,
	OPENROWSET(TABLE DM_DB_INTERNAL_ATS_DIM_STATE_NAMES) AS sn
WHERE r.id = ir.id AND r.[type_id] = 0 AND sn.[state] = r.[state]

