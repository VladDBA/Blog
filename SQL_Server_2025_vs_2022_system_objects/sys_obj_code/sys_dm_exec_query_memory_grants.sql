use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.dm_exec_query_memory_grants AS
	SELECT A.session_id, A.request_id, A.scheduler_id, A.dop, A.request_time, A.grant_time,
	A.requested_memory_kb, A.granted_memory_kb, A.required_memory_kb, A.used_memory_kb, A.max_used_memory_kb,
	A.query_cost, A.timeout_sec,
	convert(smallint, A.is_small) as resource_semaphore_id,
	A.queue_id, A.wait_order, A.is_next_candidate, A.wait_time_ms,
	A.plan_handle, A.sql_handle,
	A.group_id, A.pool_id, A.is_small, A.ideal_memory_kb,
	A.reserved_worker_count, A.used_worker_count, A.max_used_worker_count, A.reserved_node_bitmap,
	A.query_hash, A.query_plan_hash
	FROM OpenRowset(TABLE DM_EXEC_QE_GRANTSINFO) A


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.dm_exec_query_memory_grants AS
	SELECT A.session_id, A.request_id, A.scheduler_id, A.dop, A.request_time, A.grant_time,
	A.requested_memory_kb, A.granted_memory_kb, A.required_memory_kb, A.used_memory_kb, A.max_used_memory_kb,
	A.query_cost, A.timeout_sec,
	convert(smallint, A.is_small) as resource_semaphore_id,
	A.queue_id, A.wait_order, A.is_next_candidate, A.wait_time_ms,
	A.plan_handle, A.sql_handle,
	A.group_id, A.pool_id, A.is_small, A.ideal_memory_kb,
	A.reserved_worker_count, A.used_worker_count, A.max_used_worker_count, A.reserved_node_bitmap
	FROM OpenRowset(TABLE DM_EXEC_QE_GRANTSINFO) A

