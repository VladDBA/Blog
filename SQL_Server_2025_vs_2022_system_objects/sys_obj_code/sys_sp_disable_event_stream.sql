use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE   PROCEDURE sys.sp_disable_event_stream
AS
BEGIN
	DECLARE @db_name SYSNAME
	DECLARE @db_id INT
	DECLARE @swuser_flag BIT
	DECLARE @action NVARCHAR(1000)
	DECLARE @logmessage NVARCHAR(4000)
	DECLARE @check_to_use_internal_tables BIT

	SET NOCOUNT ON
	SET @db_name = DB_NAME()
	SET @db_id = DB_ID()
	SET @swuser_flag = 0

	BEGIN TRY
		SET @action = N'Performing validation'
		SET @logmessage = CONCAT(N'Performing validation. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

		-- Verify SynapseLinkPublishing FS is enabled.
		DECLARE @is_synapse_link_publishing_fs_enabled BIT = 0
		EXEC sys.sp_is_feature_enabled N'SynapseLinkPublishing',
									13802 /*TRCFLG_RETAIL_DISABLE_SYNAPSE_LINK_PUBLISHING*/,
									0 /*disable TF*/,
									@is_synapse_link_publishing_fs_enabled OUTPUT

		IF (@is_synapse_link_publishing_fs_enabled = 0)
		BEGIN
			-- ERROR: SYNAPSE_LINK_FEATURE_NOT_ENABLED
			RAISERROR(22701, 16, 7)
		END

		-- Verify permissions and that CES is not already disabled for the database.
		EXEC sys.sp_change_streams_validation

		SET @logmessage = CONCAT(N'Successfully completed validation. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

		-- Kill any ongoing jobs for this database and mark the in-memory objects as disabled to speed
		-- up the process of removing in-memory objects. Without this, we might run into a situation where
		-- long running capture job might hold the in-memory objects for longer than it is needed.
		SET @action = N'sys.sp_synapse_link_abort_all_jobs'
		EXEC sys.sp_synapse_link_abort_all_jobs 0 /*isReseedFlag*/
		SET @logmessage = CONCAT(N'Aborting all jobs. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

		-- Simulate a timeout if on a test path.
		EXEC [sys].[sp_simulate_timeout_on_test_path]

		BEGIN TRAN
		SAVE TRAN xact_sp_disable_event_stream

		--  Get Exclusive database lock.
		DECLARE @resource NVARCHAR(255)
		DECLARE @applock_result INT
		SET @action = N'sys.sp_getapplock'
		SET @resource = N'__$synapse_link__db_' + CONVERT(NVARCHAR(10), @db_id)
		EXEC @applock_result = sys.sp_getapplock @Resource = @resource, @LockMode = N'Exclusive', @LockOwner = 'Transaction', @DbPrincipal = 'public'

		IF (@applock_result < 0)
		BEGIN
			-- Lock request failed.
			SET @action = N'sys.sp_getapplock @Resource = ' + @resource + N' @LockMode = N''Exclusive'', @LockOwner = ''Transaction'', @DbPrincipal = ''public'' ' 

			-- ERROR: CHANGE_STREAMS_APPLOCK_REQUEST_NOT_GRANTED
			RAISERROR(23654, 16, -1, @action, @applock_result)
		END

		-- The lock request was granted.
		SET @logmessage = CONCAT(N'The lock request was granted. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

		-- Verify permissions.
		IF HAS_PERMS_BY_NAME(@db_name, 'DATABASE', 'CONTROL') = 0
		BEGIN
			-- ERROR: CHANGE_STREAMS_USER_NOT_PERMITTED
			RAISERROR (23624, 16, 4)
		END

		-- Ensure that CES is not already disabled before proceeding.
		IF (sys.fn_ces_is_enabled_for_current_db () = 0)
		BEGIN
			-- If database is disabled now, another thread was attempting to disable CES
			-- at the same time. Raise an informational error.
			COMMIT TRAN

			-- ERROR: CHANGE_STREAMS_NOT_ENABLED
			RAISERROR(23636, 10, 4, @db_name)
			RETURN 0
		END

		EXEC sys.sp_change_streams_check_to_use_internal_tables @check_to_use_internal_tables OUTPUT

		-- Destroy stream groups for DB.
		DECLARE @streamGroupCount INT
		DECLARE @stream_group_id UNIQUEIDENTIFIER
		DECLARE @stream_group_name SYSNAME
		DECLARE @skip_object_cleanup BIT = 0
		IF (@check_to_use_internal_tables = 1)
		BEGIN
			SELECT @streamGroupCount = COUNT(*) FROM [sys].[change_feed_table_groups] WHERE enabled <> 0
			SET @logmessage = CONCAT(N'Total number of stream groups inside DB ID: ', @db_id, N' - ', @streamGroupCount)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

			WHILE EXISTS (SELECT TOP 1 * FROM [sys].[change_feed_table_groups] WHERE enabled <> 0)
			BEGIN
				SET @action = N'sp_drop_event_stream_group_internal'
				SELECT TOP 1 @stream_group_id = table_group_id, @stream_group_name = table_group_name FROM sys.change_feed_table_groups WHERE enabled <> 0

				SET @logmessage = CONCAT(N'Executing drop stream group internal proc. DB ID: ', @db_id, N' Stream Group ID: ', @stream_group_id)
				EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

				EXEC [sys].[sp_drop_event_stream_group_internal] @stream_group_name, 0 /*wait*/, @skip_object_cleanup

				SET @logmessage = CONCAT(N'Completed drop stream group internal proc. DB ID: ', @db_id, N' Stream Group ID: ', @stream_group_id)
				EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage
			END
		END
		ELSE IF(OBJECT_ID('changefeed.change_feed_table_groups') IS NOT NULL)
		BEGIN
			SELECT @streamGroupCount = COUNT(*) FROM [changefeed].[change_feed_table_groups] WHERE enabled <> 0
			SET @logmessage = CONCAT(N'Total number of stream groups inside DB ID: ', @db_id, N' - ', @streamGroupCount)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

			WHILE EXISTS (SELECT TOP 1 * FROM [changefeed].[change_feed_table_groups] WHERE enabled <> 0)
			BEGIN
				SET @action = N'sp_drop_event_stream_group_internal'
				SELECT TOP 1 @stream_group_id = table_group_id, @stream_group_name = table_group_name FROM changefeed.change_feed_table_groups WHERE enabled <> 0

				SET @logmessage = CONCAT(N'Executing drop stream group internal proc. DB ID: ', @db_id, N' Stream Group ID: ', @stream_group_id)
				EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

				EXEC [sys].[sp_drop_event_stream_group_internal] @stream_group_name, 0 /*wait*/, @skip_object_cleanup

				SET @logmessage = CONCAT(N'Completed drop stream group internal proc. DB ID: ', @db_id, N' Stream Group ID: ', @stream_group_id)
				EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage
			END
		END

		-- Before resetting the replication bit for the database, verify no tables are present in sys.tables that have is_replicated = 1
		-- Otherwise this will cause error 3941 on any DML against these tables after disable db proc is completed.
		-- Do not rely on change_feed_tables table to disable changefeed on all tables as following scenarios can lead to 3941
		--  1. Users manually removing entries from change_feed_tables
		--  2. Enable table proc running into client timeout and retry puts the table in a state 
		--     where is_replicated is set to 1 but corresponding entry went missing from change_feed_tables.
		--
		DECLARE @object_id INT
		DECLARE @table_resource NVARCHAR(255)
		WHILE EXISTS (SELECT TOP 1 * FROM sys.tables WHERE is_replicated = 1 AND is_ms_shipped = 0)
		BEGIN
			SELECT TOP 1 @object_id = object_id FROM sys.tables where is_replicated = 1 AND is_ms_shipped = 0
			IF(@object_id IS NOT NULL)
			BEGIN
				-- Get exclusive lock.
				-- The naming convention should be used across all stored proc that
				-- require exclusive access to modify the database tables.
				SET @table_resource = N'__$synapse_link__table_' + CONVERT(NVARCHAR(10), @object_id)
				EXEC @applock_result = sys.sp_getapplock @Resource = @table_resource, @LockMode = N'Exclusive', @LockOwner = 'Transaction', @DbPrincipal = 'public'

				IF (@applock_result < 0)
				BEGIN
					-- Lock request failed.
					SET @action = N'sys.sp_getapplock @Resource = ' + @table_resource + N' @LockMode = N''Exclusive'', @LockOwner = ''Transaction'', @DbPrincipal = ''public'' ' 

					-- ERROR: CHANGE_STREAMS_APPLOCK_REQUEST_NOT_GRANTED
					RAISERROR (23654, 16, -1, @action, @applock_result)
				END

				-- Set table as not replicated.
				SET @action = 'sys.sp_change_streams_disable_object_replication'
				EXEC sys.sp_change_streams_disable_object_replication @object_id

				IF (@check_to_use_internal_tables = 1)
				BEGIN
					SET @action = 'Delete from [sys].[change_feed_tables] where object_id = @object_id and state = 5'
					DELETE FROM [sys].[change_feed_tables] WHERE object_id = @object_id AND STATE = 5 -- State 5 means that table is disabled.
				END
				ELSE IF (OBJECT_ID(N'[changefeed].[change_feed_tables]') IS NOT NULL)
				BEGIN
					EXECUTE AS USER = 'changefeed'
					SET @swuser_flag = 1

					SET @action = 'Delete from [changefeed].[change_feed_tables] where object_id = @object_id and state = 5'
					DELETE FROM [changefeed].[change_feed_tables] WHERE object_id = @object_id AND state = 5 -- State 5 means that table is disabled.

					REVERT
					SET @swuser_flag = 0
				END

				SET @logmessage = CONCAT(N'Table with object ID: ', @object_id, N' unmarked for replication. DB ID: ', @db_id)
				EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage
			END
		END

		-- Once the control reaches here, all topics and tables are already cleaned from memory and
		-- no jobs should be enqueued for this db.
		DECLARE @flush_proc NVARCHAR(300)
		DECLARE @done_proc NVARCHAR(300)
		DECLARE @clearcache_proc NVARCHAR(300)
		SELECT @flush_proc = N'sys.sp_replflush'
		SELECT @done_proc  = N'sys.sp_repldone'
		SELECT @clearcache_proc = N'sys.sp_replhelp'

		SET @action = N'sys.sp_replflush'
		EXEC @flush_proc
		SET @logmessage = CONCAT(N'sp_replflush. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

		DECLARE @retcode INT
		DECLARE @is_replication_post_vlr_for_repldone_reset_change_feed_fs_enabled INT = 0
		EXEC sys.sp_is_feature_enabled N'ReplicationPostVLRForReplDoneResetChangeFeed',
										8257 /*TRCFLG_RETAIL_REPLICATION_POST_VLR_REPLDONE_RESET_CHANGEFEED*/,
										1 /*enable TF*/,
										@is_replication_post_vlr_for_repldone_reset_change_feed_fs_enabled OUTPUT

		SET @action = N'sys.sp_repldone NULL, NULL, 0, 0, 1, 0, ' + CONVERT(nvarchar(1), @is_replication_post_vlr_for_repldone_reset_change_feed_fs_enabled)
		EXEC @retcode = @done_proc NULL, NULL, 0, 0, 1, 0, @is_replication_post_vlr_for_repldone_reset_change_feed_fs_enabled
		IF (@@ERROR <> 0 OR @retcode <> 0)
		BEGIN
			SET @logmessage = CONCAT(N'sp_repldone failed. DB ID: ', @db_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

			-- ERROR: REPL_DONE_FAILED
			RAISERROR(22912, 16, -1)
		END
		SET @logmessage = CONCAT(N'sp_repldone. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

		SET @action = N'sys.sp_replflush'
		EXEC @flush_proc
		SET @logmessage = CONCAT(N'sp_replflush. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

		SET @action = N'sys.sp_replhelp N''ClearDbArticleCache'''
		EXEC @clearcache_proc N'ClearDbArticleCache' -- Clear article cache for this database.
		SET @logmessage = CONCAT(N'ClearDbArticleCache. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

		SET @action = N'sys.sp_replhelp N''DisablePerDbHistoryCache'''
		EXEC @clearcache_proc N'DisablePerDbHistoryCache' -- Clear DMV cache for this database.
		SET @logmessage = CONCAT(N'DisablePerDbHistoryCache. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

		IF (@check_to_use_internal_tables = 1)
		BEGIN
			-- Truncate metadata tables.
			IF EXISTS
			(
				SELECT *
				FROM sys.objects
				WHERE name = N'systranschemas'
				AND ObjectProperty(OBJECT_ID('sys.systranschemas'), 'IsTable') = 1
			)
			BEGIN
				SET @action = N'TRUNCATE TABLE sys.systranschemas'
				EXEC sp_trident_link_truncate_internal_tables N'systranschemas'
			END
			SET @action = N'TRUNCATE Trident Link Metadata Tables'
			IF OBJECT_ID('[sys].[change_feed_tables]') IS NOT NULL
			BEGIN
				EXEC sp_trident_link_truncate_internal_tables N'change_feed_tables'
			END
			IF OBJECT_ID('[sys].[change_feed_table_groups]') IS NOT NULL
			BEGIN
				EXEC sp_trident_link_truncate_internal_tables N'change_feed_table_groups'
			END
			IF OBJECT_ID('[sys].[change_feed_settings]') IS NOT NULL
			BEGIN
				EXEC sp_trident_link_truncate_internal_tables N'change_feed_settings'
			END
			SET @logmessage = CONCAT(N'Truncated all metadata internal tables. DB ID: ', @db_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage
		END
		ELSE
		BEGIN
			EXEC sp_set_drop_meta_objects_allowed

			-- Switch to database user 'changefeed' to drop tables.
			EXECUTE AS USER = 'changefeed'
			SET @swuser_flag = 1

			IF EXISTS
			(
				SELECT *
				FROM sys.objects
				WHERE name = N'systranschemas'
				AND ObjectProperty(OBJECT_ID('systranschemas'), 'IsTable') = 1
			)
			BEGIN
				SET @action = N'DROP TABLE dbo.systranschemas'
				DROP TABLE dbo.systranschemas
			END

			-- Drop metadata tables.
			SET @action = N'Drop Synapse Link Metadata Tables'
			EXEC(N'DROP TABLE [changefeed].[change_feed_tables]')
			EXEC(N'DROP TABLE [changefeed].[change_feed_table_groups]')
			EXEC(N'DROP TABLE [changefeed].[change_feed_settings]')

			EXEC sp_unset_drop_meta_objects_allowed

			SET @logmessage = CONCAT(N'Dropped all metadata tables. DB ID: ', @db_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage
		END

		-- Mark the database as disabled for Change Event Stream.
		SET @action = N'SetCes(Value = 0)'

		-- For sql Azure, when you connect to backend, the DB_name() is logical database name, but the name from sys.databases is physical database name.
		-- so when we want to set CES to off, we shall use the PDB name.
		DECLARE @physical_db_name SYSNAME
		SELECT @physical_db_name = name FROM sys.databases WHERE database_id = @db_id

		IF (SERVERPROPERTY('EngineEdition') != 5 AND @db_name <> @physical_db_name)
		BEGIN
			EXEC %%DatabaseEx(Name = @physical_db_name).SetCes(Value = 0)
		END
		ELSE
		BEGIN
			EXEC %%DatabaseEx(Name = @db_name).SetCes(Value = 0)
		END
		SET @logmessage = CONCAT(N'Reset Replicated bit for the database to disable CES. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

		IF (@check_to_use_internal_tables = 0 AND DATABASE_PRINCIPAL_ID ('changefeed') IS NOT NULL)
		BEGIN
			-- Drop the changefeed schema and user if they exist
			IF (SCHEMA_ID ('changefeed') IS NOT NULL)
			BEGIN
				SET @action = N'Drop schema [changefeed]'
				DROP SCHEMA changefeed
				SET @logmessage = CONCAT(N'Dropped changefeed schema. DB ID: ', @db_id)
				EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage
			END

			REVERT
			SET @swuser_flag = 0

			EXECUTE AS USER = 'dbo'
			SET @swuser_flag = 1

			SET @action = N'Drop user [changefeed]'
			DROP USER changefeed
			SET @logmessage = CONCAT(N'Dropped changefeed user. DB ID: ', @db_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

			REVERT
			SET @swuser_flag = 0
		END

		COMMIT TRAN

		SET @logmessage = CONCAT(N'Committed transaction inside the TRY block. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage
	END TRY

	BEGIN CATCH
		SET @logmessage = CONCAT(N'Entering CATCH block. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage

		IF (XACT_STATE() = -1)
		BEGIN
			ROLLBACK TRAN
			SET @logmessage = concat(N'Transaction rolled back. DB ID: ', @db_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage
		END
		ELSE IF (XACT_STATE() = 1)
		BEGIN
			ROLLBACK TRAN xact_sp_disable_event_stream
			COMMIT TRAN
			SET @logmessage = CONCAT(N'Transaction rolled back to savepoint: xact_sp_disable_event_stream. DB ID: ', @db_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_disable_event_stream', @logmessage
		END

		-- Save the error number and associated message raised in the TRY block.
		DECLARE @raised_error INT, @raised_state INT, @raised_message NVARCHAR(4000)
		SELECT @raised_error = ERROR_NUMBER()
		SELECT @raised_state = ERROR_STATE()
		SELECT @raised_message = ERROR_MESSAGE()

		IF (@swuser_flag = 1)
		BEGIN
			REVERT
		END

		EXEC sp_unset_drop_meta_objects_allowed

		SET @logmessage = CONCAT(N'Caught an exception while performing action: ', @action,
			N'. DB ID: ', @db_id, 
			N'. Error code: ', @raised_error, 
			N'. Error state: ', @raised_state)
		EXEC sys.sp_synapse_link_fire_trace_event @raised_error, N'sp_disable_event_stream', @logmessage

		-- ERROR: CHANGE_STREAMS_STORED_PROC_FAILED
		RAISERROR (23626, 16, 6, @raised_error, @raised_state, @raised_message)

		RETURN 1
	END CATCH

	EXEC sp_unset_drop_meta_objects_allowed

	RETURN 0
END

