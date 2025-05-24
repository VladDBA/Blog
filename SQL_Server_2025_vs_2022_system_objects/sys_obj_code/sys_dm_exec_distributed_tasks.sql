SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
CREATE VIEW sys.dm_exec_distributed_tasks AS
	SELECT
		session_id, request_id, start_time, cpu_time, total_elapsed_time,
		distributed_statement_id, distributed_query_hash, distributed_request_id,
		distributed_scheduler_id, distributed_query_operator_id,
		distributed_task_group_id, distributed_execution_id, distributed_submission_id
	FROM OpenRowset(TABLE SYSDISTRIBUTEDTASKS)
	WHERE distributed_statement_id IS NOT NULL

