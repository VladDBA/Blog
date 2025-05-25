use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE view sys.dm_change_feed_errors
as
	SELECT 
	    [session_id]
		,[source_task]
		,[table_group_id]
		,[table_id]
        ,[capture_phase_number]
        ,[entry_time]
        ,[error_number]
		,iif(error_severity <= 0, severity, error_severity) as [error_severity]
        ,[error_state]
        ,isnull([error_message], text) as [error_message]
        ,[batch_start_lsn]
		,[batch_end_lsn]
		,[tran_begin_lsn]
		,[tran_commit_lsn]
        ,[sequence_value]
		,[command_id]
	FROM OpenRowset(TABLE DM_CHANGE_FEED_ERRORS) left outer join sys.messages on error_number = message_id and language_id = CAST(SERVERPROPERTY('LCID') as int)


/*====  SQL Server 2022 version  ====*/
CREATE view sys.dm_change_feed_errors
as
	SELECT 
	    [session_id]
		,[source_task]
		,[table_group_id]
		,[table_id]
        ,[capture_phase_number]
        ,[entry_time]
        ,[error_number]
        ,[error_severity]
        ,[error_state]
        ,isnull([error_message], text) as [error_message]
        ,[batch_start_lsn]
		,[batch_end_lsn]
		,[tran_begin_lsn]
		,[tran_commit_lsn]
        ,[sequence_value]
		,[command_id]
	FROM OpenRowset(TABLE DM_CHANGE_FEED_ERRORS) left outer join sys.messages on error_number = message_id and language_id = CAST(SERVERPROPERTY('LCID') as int)

