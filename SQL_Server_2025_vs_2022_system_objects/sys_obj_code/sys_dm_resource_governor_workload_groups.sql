SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.dm_resource_governor_workload_groups AS
	SELECT
		group_id,
		name,
		pool_id,
		external_pool_id,
		statistics_start_time,
		total_request_count,
		total_queued_request_count,
		active_request_count,
		queued_request_count,
		total_cpu_limit_violation_count,
		total_cpu_usage_ms,
		max_request_cpu_time_ms,
		blocked_task_count,
		total_lock_wait_count,
		total_lock_wait_time_ms,
		total_query_optimization_count,
		total_suboptimal_plan_generation_count,
		total_reduced_memgrant_count,
		max_request_grant_memory_kb,
		active_parallel_thread_count,
		importance,
		isnull(cast(request_max_memory_grant_percent as int), 0) request_max_memory_grant_percent,
		request_max_cpu_time_sec,
		request_memory_grant_timeout_sec,
		group_max_requests,
		max_dop,
		effective_max_dop,
		total_cpu_usage_preemptive_ms,
		isnull(cast(request_max_memory_grant_percent as float), 0) request_max_memory_grant_percent_numeric,
		total_cpu_usage_actual_ms,
		cache_memory_kb,
		compile_memory_kb,
		used_memory_kb,
		try_cast(cap_cpu_percent as decimal(5,2)) cap_cpu_percent,
		tempdb_data_space_kb,
		peak_tempdb_data_space_kb,
		total_tempdb_data_limit_violation_count
	FROM OpenRowSet(TABLE DM_RG_GROUPS, 1)


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.dm_resource_governor_workload_groups AS
	SELECT
		group_id,
		name,
		pool_id,
		external_pool_id,
		statistics_start_time,
		total_request_count,
		total_queued_request_count,
		active_request_count,
		queued_request_count,
		total_cpu_limit_violation_count,
		total_cpu_usage_ms,
		max_request_cpu_time_ms,
		blocked_task_count,
		total_lock_wait_count,
		total_lock_wait_time_ms,
		total_query_optimization_count,
		total_suboptimal_plan_generation_count,
		total_reduced_memgrant_count,
		max_request_grant_memory_kb,
		active_parallel_thread_count,
		importance,
		isnull(cast(request_max_memory_grant_percent as int), 0) request_max_memory_grant_percent,
		request_max_cpu_time_sec,
		request_memory_grant_timeout_sec,
		group_max_requests,
		max_dop,
		effective_max_dop,
		total_cpu_usage_preemptive_ms,
		isnull(cast(request_max_memory_grant_percent as float), 0) request_max_memory_grant_percent_numeric
	FROM OpenRowSet(TABLE DM_RG_GROUPS, 1)

