use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE VIEW sys.dm_exec_requests_history AS
	SELECT
		status =
		CASE status
			WHEN 0 THEN 'Completed'
			WHEN 1 THEN 'Canceled'
			ELSE 'Failed'
		END,
		transaction_id,
		distributed_statement_id,
		query_hash,
		login_name,
		start_time,
		end_time,
		command =
		CASE WHEN command IS NULL
			 THEN NULL
			 ELSE getcommandname(command)
		END,
		CASE WHEN (query_info & 1 != 0) THEN '*** Global stats query ***'
			 WHEN (query_info & 2 != 0) THEN '*** Internal delta query ***'
			 WHEN (query_info & 8 != 0) THEN '*** Internal cardinality query ***'
			 WHEN (query_info & 16 != 0) THEN '*** External table stats query ***'
			 WHEN (query_info & 32 != 0) THEN '*** Internal CSV parsing query ***'
			 WHEN (req.statement_offset_start IS NULL OR req.statement_offset_end IS NULL OR req.statement_offset_start < -1 OR req.statement_offset_end < -1 OR req.statement_offset_start > req.statement_offset_end) THEN '*** Query text not generated ***'
			 ELSE CASE WHEN sql_text IS NOT NULL THEN
				SUBSTRING(
				sql_text,
				(req.statement_offset_start / 2) + 1,
					(
						(
							CASE req.statement_offset_end
								WHEN -1 THEN DATALENGTH(sql_text)
								ELSE req.statement_offset_end
							END - req.statement_offset_start
						) / 2
					) + 1
				)
				ELSE '*** Query text unavailable ***'
				END
		END AS query_text,
		total_elapsed_time_ms,
		data_processed_mb,
		error,
		error_code =
		CASE WHEN error_code IS NULL
			 THEN 0
			 ELSE error_code
		END,
		rejected_rows_path
	FROM
		master.sys.polaris_executed_requests_history req
		LEFT JOIN master.sys.polaris_executed_requests_text txt ON req.sql_handle = txt.sql_handle


/*====  SQL Server 2022 version  ====*/
CREATE VIEW sys.dm_exec_requests_history AS
	SELECT 
		status =
		CASE status
			WHEN 0 THEN 'Completed'
			WHEN 1 THEN 'Canceled'
			ELSE 'Failed'
		END, 
		transaction_id,
		distributed_statement_id,
		query_hash,
		login_name,
		start_time,
		end_time,
		command =
		CASE WHEN command IS NULL
			 THEN NULL
			 ELSE getcommandname(command)
		END,
		CASE WHEN (query_info & 1 != 0) THEN '*** Global stats query ***'
			 WHEN (query_info & 2 != 0) THEN '*** Internal delta query ***'
			 WHEN (query_info & 8 != 0) THEN '*** Internal cardinality query ***'
			 WHEN (query_info & 16 != 0) THEN '*** External table stats query ***'
			 WHEN (req.statement_offset_start IS NULL OR req.statement_offset_end IS NULL OR req.statement_offset_start < -1 OR req.statement_offset_end < -1 OR req.statement_offset_start > req.statement_offset_end) THEN '*** Query text not generated ***'
			 ELSE CASE WHEN sql_text IS NOT NULL THEN
				SUBSTRING(
				sql_text,
				(req.statement_offset_start / 2) + 1,
					(
						(
							CASE req.statement_offset_end
								WHEN -1 THEN DATALENGTH(sql_text)
								ELSE req.statement_offset_end
							END - req.statement_offset_start
						) / 2
					) + 1
				)
				ELSE '*** Query text unavailable ***'
				END
		END AS query_text,
		total_elapsed_time_ms,
		data_processed_mb,
		error,
		error_code = 
		CASE WHEN error_code IS NULL
			 THEN 0
			 ELSE error_code
		END,
		rejected_rows_path
	FROM 
		master.sys.polaris_executed_requests_history req
		LEFT JOIN master.sys.polaris_executed_requests_text txt ON req.sql_handle = txt.sql_handle

