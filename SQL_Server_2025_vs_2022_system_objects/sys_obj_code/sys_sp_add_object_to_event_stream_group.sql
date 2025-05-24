SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE   PROCEDURE sys.sp_add_object_to_event_stream_group
	@stream_group_name SYSNAME,
	@object_name NVARCHAR(512), -- Should contain the schema name.
	@include_all_columns BIT = 1,
	@include_old_values BIT = 1,
	@include_old_lob_values BIT = 0
AS
BEGIN
SET NOCOUNT ON
	SET XACT_ABORT ON

	DECLARE @action NVARCHAR(1000)
	DECLARE @logmessage NVARCHAR(4000)
	DECLARE @db_name SYSNAME
	DECLARE @db_id INT
	DECLARE @table_id UNIQUEIDENTIFIER
	DECLARE @stream_group_id UNIQUEIDENTIFIER
	DECLARE @swuser_flag BIT

	SET @db_name = DB_NAME()
	SET @db_id = DB_ID()
	SET @table_id = NEWID()
	SET @stream_group_id = '00000000-0000-0000-0000-000000000000'
	SET @swuser_flag = 0

	BEGIN TRY
		SET @action = N'Validating parameters'
		SET @logmessage = CONCAT(N'Starting parameter validation. Table ID: ', @table_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_add_object_to_event_stream_group', @logmessage

		-- Checks pemissions and validates that CES is enabled.
		EXEC sys.sp_change_streams_validation

		-- Validate @object_name. Function throws on Error.
		EXEC sys.sp_change_streams_object_name_validation @object_name

		-- Get Object Id and Object schema from @object_name.
		DECLARE @source_object_id INT
		DECLARE @source_object NVARCHAR(1000)

		-- In case when include_old_values is set to false, we won't send
		-- old values in the event row, so there is no point in allowing
		-- include_old_lob_values option, as it won't have any effect.
		IF (@include_old_values = 0 AND @include_old_lob_values = 1)
		BEGIN
			-- ERROR: CHANGE_STREAMS_INVALID_ARGUMENT_DIFFERENT_VALUE_EXPECTED
			RAISERROR (23641, 16, 1, 'include_old_lob_values', 'include_old_values', '1')
		END

		-- In case when include_all_columns is set to false, we will always send
		-- updated columns in the event row, including lob columns, so there
		-- is no point in allowing include_old_lob_values option, as it won't have
		-- any effect.
		IF (@include_all_columns = 0 AND @include_old_lob_values = 1)
		BEGIN
			-- ERROR: CHANGE_STREAMS_INVALID_ARGUMENT_DIFFERENT_VALUE_EXPECTED
			RAISERROR (23641, 16, 2, 'include_old_lob_values', 'include_all_columns', '1')
		END

		SELECT @source_object = @object_name

		SELECT @source_object_id = OBJECT_ID(@source_object)

		DECLARE @object_schema SYSNAME

		SET @object_schema = OBJECT_SCHEMA_NAME(@source_object_id)

		-- External tables are currently not supported.
		IF EXISTS (SELECT 1 FROM sys.tables WHERE object_id = @source_object_id AND is_external = 1)
		BEGIN
			-- ERROR: CHANGE_STREAMS_BLOCK_FOR_EXTERNAL_TABLES
			RAISERROR(23656, 16, -1, @object_name)
		END

		EXEC [sys].[sp_change_feed_vupgrade]

		-- Simulate a timeout if on a test path.
		EXEC [sys].[sp_simulate_timeout_on_test_path]

		BEGIN TRAN
		SAVE TRAN change_streams_add_object

		-- Get Shared database lock.
		DECLARE @resource NVARCHAR(255)
		DECLARE @applock_result INT
		SET @action = N'sys.sp_getapplock'
		SET @resource = N'__$synapse_link__db_' + CONVERT(NVARCHAR(10), @db_id)
		EXEC @applock_result = sys.sp_getapplock @Resource = @resource, @LockMode = N'Shared', @LockOwner = 'Transaction', @DbPrincipal = 'public'

		IF (@applock_result < 0)
		BEGIN
			-- Lock request failed.
			SET @action = N'sys.sp_getapplock @Resource = ' + @resource + N' @LockMode = N''Shared'', @LockOwner = ''Transaction'', @DbPrincipal = ''public'' '

			-- ERROR: CHANGE_STREAMS_APPLOCK_REQUEST_NOT_GRANTED
			RAISERROR(23654, 16, -1, @action, @applock_result)
		END

		-- Get Exclusive table lock.
		DECLARE @table_resource NVARCHAR(255)
		SET @table_resource = N'__$synapse_link__table_' + CONVERT(NVARCHAR(10), @source_object_id)
		EXEC @applock_result = sys.sp_getapplock @Resource = @table_resource, @LockMode = N'Exclusive', @LockOwner = 'Transaction', @DbPrincipal = 'public'

		IF (@applock_result < 0)
		BEGIN
			-- Lock request failed.
			SET @action = N'sys.sp_getapplock @Resource = ' + @table_resource + N' @LockMode = N''Exclusive'', @LockOwner = ''Transaction'', @DbPrincipal = ''public'' ' 

			-- ERROR: CHANGE_STREAMS_APPLOCK_REQUEST_NOT_GRANTED
			RAISERROR(23654, 16, -1, @action, @applock_result)
		END

		-- The lock request was granted.
		SET @logmessage = CONCAT(N'The lock request was granted. Object ID: ', @source_object_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_add_object_to_event_stream_group', @logmessage

		-- Checks pemissions and validates that CES is enabled.
		EXEC sys.sp_change_streams_validation

		-- Validate @stream_group_name. Function throws on Error.
		-- Checks @stream_group_name is not null and exists in the right tables.
		EXEC sys.sp_change_streams_stream_group_name_validation @stream_group_name

		-- Get the stream_group_id from the stream_group_name.
		DECLARE @check_to_use_internal_tables BIT
		EXEC sys.sp_change_streams_check_to_use_internal_tables @check_to_use_internal_tables OUTPUT

		IF (@check_to_use_internal_tables = 1)
		BEGIN
			SELECT @stream_group_id = table_group_id
			FROM sys.change_feed_table_groups
			WHERE table_group_name = @stream_group_name
				AND destination_type = 1
		END
		ELSE
		BEGIN
			SELECT @stream_group_id = table_group_id
			FROM changefeed.change_feed_table_groups
			WHERE table_group_name = @stream_group_name
				AND destination_type = 1
		END

		-- Verify that CES is not already enabled for given table,
		-- and that limit of 40K tables per stream group is not reached.
		DECLARE @tablecnt INT

		IF (@check_to_use_internal_tables = 1)
		BEGIN
			IF EXISTS (
				SELECT *
				FROM sys.change_feed_tables
				WHERE table_group_id = @stream_group_id
					AND object_id = @source_object_id
					AND state <> 5 -- State 5 means that table is disabled.
			)
			BEGIN
				-- ERROR: CHANGE_STREAMS_ALREADY_ENABLED_FOR_OBJECT
				RAISERROR (23643, 16, 3, @object_name)
			END

			SELECT @tablecnt = COUNT(*) FROM [sys].[change_feed_tables] WHERE table_group_id = @stream_group_id
		END
		ELSE
		BEGIN
			IF EXISTS (
				SELECT *
				FROM changefeed.change_feed_tables
				WHERE table_group_id = @stream_group_id
					AND object_id = @source_object_id
					AND state <> 5 -- State 5 means that table is disabled.
			)
			BEGIN
				-- ERROR: CHANGE_STREAMS_ALREADY_ENABLED_FOR_OBJECT
				RAISERROR (23643, 16, 1, @object_name)
			END

			SELECT @tablecnt = COUNT(*) FROM [changefeed].[change_feed_tables] WHERE table_group_id = @stream_group_id
		END

		IF (@tablecnt >= 40000)
		BEGIN
			-- ERROR: CHANGE_STREAMS_MAX_TABLES_IN_TOPIC_EXCEEDED
			RAISERROR(23657, 16, -1, 40000, @tablecnt)
		END

		SET @logmessage = CONCAT(N'Successfully completed parameter validation. Stream Group ID: ', @stream_group_id, N'. Table ID: ', @table_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_add_object_to_event_stream_group', @logmessage

		IF (@check_to_use_internal_tables = 0)
		BEGIN
			EXECUTE AS USER = 'changefeed'
			SET @swuser_flag = 1
		END

		-- Set the parameters as replicated.
		EXEC sys.sp_change_streams_enable_object_replication @source_object_id

		SET @logmessage = CONCAT(N'Table marked for replication. Stream Group ID: ', @stream_group_id, N'. Table ID: ', @table_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_add_object_to_event_stream_group', @logmessage

		-- Set default values for version, enable_lsn and disable_lsn
		DECLARE @version BINARY(10) = 0x00000000000000000000
		DECLARE @enable_lsn BINARY(10) = 0x00000000000000000000
		DECLARE @disable_lsn BINARY(10) = 0x00000000000000000000

		DECLARE @retcode INT
		SET @action = N'sys.sp_replincrementlsn_internal @enable_lsn OUTPUT'
		EXEC @retcode = sys.sp_replincrementlsn_internal @enable_lsn OUTPUT
		SET @logmessage = CONCAT(N'sp_replincrementlsn_internal. Stream Group ID: ', @stream_group_id, N'. Table ID: ', @table_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_add_object_to_event_stream_group', @logmessage

		IF (@retcode != 0) OR (@@error != 0)
		BEGIN
			-- ERROR: SYNAPSE_LINK_METADATA_UPDATE_FAILURE
			RAISERROR(22710, 16, -1, @action, @@error, '')
		END

		DECLARE @state INT = 1

		IF (@check_to_use_internal_tables = 1)
		BEGIN
			DECLARE @stmt_to_exec nvarchar(max)
			SET @stmt_to_exec = N'
				INSERT INTO [sys].[change_feed_tables]
				(table_group_id, object_id, version, state, table_id, enable_lsn, disable_lsn, snapshot_row_count, include_old_values, include_all_columns, include_old_lob_values)
				VALUES(@table_group_id, @source_object_id, @version, @state /* enabled */, @table_id, @enable_lsn, @disable_lsn, 0 /* default is zero */, @include_old_values, @include_all_columns, @include_old_lob_values)'
			EXEC sp_executesql @stmt_to_exec,
								N'@table_group_id uniqueidentifier,
									@source_object_id int,
									@version binary(10),
									@state tinyint,
									@table_id uniqueidentifier,
									@enable_lsn binary(10),
									@disable_lsn binary(10),
									@include_old_values bit,
									@include_all_columns bit,
									@include_old_lob_values bit',
								@stream_group_id,
								@source_object_id,
								@version,
								@state /* enabled */,
								@table_id,
								@enable_lsn,
								@disable_lsn,
								@include_old_values,
								@include_all_columns,
								@include_old_lob_values
		END
		ELSE
		BEGIN
			INSERT INTO [changefeed].[change_feed_tables]
				(table_group_id, object_id, state, table_id, enable_lsn, snapshot_row_count, include_old_values, include_all_columns, include_old_lob_values)
			VALUES(@stream_group_id, @source_object_id, @state /* enabled or reseeding */, @table_id, @enable_lsn, 0 /* default is zero */, @include_old_values, @include_all_columns, @include_old_lob_values)
		END

		SET @logmessage = CONCAT(N'Inserted an entry into change_feed_tables table. Stream Group ID: ', @stream_group_id, N'. Table ID: ', @table_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_add_object_to_event_stream_group', @logmessage

		-- Call sp_replflush to notify capture process that a new table has been enabled.
		SET @action = N'sys.sp_replflush'
		EXEC sys.sp_replflush
		SET @logmessage = CONCAT(N'sp_replflush. Stream Group ID: ', @stream_group_id, N'. Table ID: ', @table_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_add_object_to_event_stream_group', @logmessage

		IF (@swuser_flag = 1)
		BEGIN
			REVERT
			SET @swuser_flag = 0
		END

		COMMIT TRAN
		SET @logmessage = CONCAT(N'Committed transaction inside the TRY block. Stream Group ID: ', @stream_group_id, N'. Table ID: ', @table_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_add_object_to_event_stream_group', @logmessage
	END TRY

	BEGIN CATCH
		SET @logmessage = CONCAT(N'Entering CATCH block. Stream Group ID: ', @stream_group_id, N'. Table ID: ', @table_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_add_object_to_event_stream_group', @logmessage

		IF (XACT_STATE() = -1)
		BEGIN
			ROLLBACK TRAN
			SET @logmessage = CONCAT(N'Transaction rolled back. Stream Group ID: ', @stream_group_id, N'. Table ID: ', @table_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_add_object_to_event_stream_group', @logmessage
		END
		ELSE IF (XACT_STATE() = 1)
		BEGIN
			ROLLBACK TRAN change_streams_add_object
			COMMIT TRAN
			SET @logmessage = CONCAT(N'Transaction rolled back to savepoint: change_streams_add_object. Stream Group ID: ', @stream_group_id, N'. Table ID: ', @table_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_add_object_to_event_stream_group', @logmessage
		END

		DECLARE @raised_error INT, @raised_state INT, @raised_message NVARCHAR(4000)

		-- Save the error number and associated message raised in the TRY block.
		SELECT @raised_error = ERROR_NUMBER()
		SELECT @raised_state = ERROR_STATE()
		SELECT @raised_message = ERROR_MESSAGE()

		IF (CURRENT_USER = 'changefeed')
		BEGIN
			REVERT
		END

		SET @logmessage = CONCAT(N'Caught an exception while performing action: ', @action,
			N'. Stream Group ID: ', @stream_group_id,
			N'. Table ID: ', @table_id,
			N'. Error code: ', @raised_error, 
			N'. Error State: ', @raised_state)
		EXEC sys.sp_synapse_link_fire_trace_event @raised_error, N'sp_add_object_to_event_stream_group', @logmessage

		-- ERROR: CHANGE_STREAMS_STORED_PROC_FAILED
		RAISERROR (23626, 16, 1, @raised_error, @raised_state, @raised_message)

		RETURN 1
	END CATCH

	RETURN 0
END

