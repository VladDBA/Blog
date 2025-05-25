SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
 
CREATE   PROCEDURE sys.sp_enable_event_stream
AS
BEGIN
	DECLARE @db_name SYSNAME
	DECLARE @db_id INT
	DECLARE @maxtrans INT
	DECLARE @pollinterval INT
	DECLARE @schema_version INT
	DECLARE @seqno BINARY(10)
	DECLARE @swuser_flag BIT
	DECLARE @action NVARCHAR(1000)
	DECLARE @logmessage NVARCHAR(4000)
	DECLARE @check_to_use_internal_tables BIT

	SET NOCOUNT ON
	SET @db_name = DB_NAME()
	SET @db_id = DB_ID()
	SET @maxtrans = 500
	SET @pollinterval = 5
	SET @schema_version = 8
	SET @seqno = 0x00000000000000000000
	SET @swuser_flag = 0

	BEGIN TRY
		SET @action = N'Performing validation'
		SET @logmessage = CONCAT(N'Performing validation. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_enable_event_stream', @logmessage

		-- Verify SynapseLinkPublishing FS is enabled.
		DECLARE @is_synapse_link_publishing_fs_enabled BIT = 0
		EXEC sys.sp_is_feature_enabled N'SynapseLinkPublishing',
									13802 /*TRCFLG_RETAIL_DISABLE_SYNAPSE_LINK_PUBLISHING*/,
									0 /*disable TF*/,
									@is_synapse_link_publishing_fs_enabled OUTPUT

		IF (@is_synapse_link_publishing_fs_enabled = 0)
		BEGIN
			-- ERROR: SYNAPSE_LINK_FEATURE_NOT_ENABLED
			RAISERROR(22701, 16, 6)
		END

		-- Verify ChangeFeedEventHubIntegration FS is enabled.
		DECLARE @is_change_feed_event_hub_integration_fs_enabled BIT = 0
		EXEC sys.sp_is_feature_enabled N'ChangeFeedEventHubIntegration',
										13810 /*TRCFLG_RETAIL_DISABLE_CHANGE_FEED_TO_EVENT_HUB*/,
										0 /*disable TF*/,
										@is_change_feed_event_hub_integration_fs_enabled OUTPUT

		IF (@is_change_feed_event_hub_integration_fs_enabled = 0)
		BEGIN
			-- ERROR: CHANGE_STREAMS_NOT_SUPPORTED
			RAISERROR (23634, 16, 2)
		END

		-- Verify permissions.
		IF HAS_PERMS_BY_NAME(@db_name, 'DATABASE', 'CONTROL') = 0
		BEGIN
			-- ERROR: CHANGE_STREAMS_USER_NOT_PERMITTED
			RAISERROR (23624, 16, 2)
		END

		-- Update @maxtrans value if needed.
		DECLARE @is_replication_use_10k_as_max_trans_fs_enabled BIT = 0
		EXEC sys.sp_is_feature_enabled N'ReplicationUse10kAsMaxTrans',
									8254 /*TRCFLG_RETAIL_REPLICATION_USE_10K_MAXTRANS*/,
									1 /*enable TF*/,
									@is_replication_use_10k_as_max_trans_fs_enabled OUTPUT

		IF (@is_replication_use_10k_as_max_trans_fs_enabled = 1)
		BEGIN
			SET @maxtrans = 10000
		END

		-- Verify that database is not system database.
		IF (@db_name = N'model' OR @db_name = N'msdb' OR @db_name = N'master' OR @db_name = N'tempdb')
		BEGIN
			-- ERROR: CHANGE_STREAMS_CANT_ENABLE_SYSTEM_DB
			RAISERROR(23648, 16, 1, @db_name)
		END

		-- Simulate a timeout if on a test path.
		EXEC [sys].[sp_simulate_timeout_on_test_path]

		BEGIN TRAN
		SAVE TRAN xact_sp_enable_event_stream

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
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_enable_event_stream', @logmessage

		-- Verify permissions.
		IF HAS_PERMS_BY_NAME(@db_name, 'DATABASE', 'CONTROL') = 0
		BEGIN
			-- ERROR: CHANGE_STREAMS_USER_NOT_PERMITTED
			RAISERROR (23624, 16, 3)
		END

		-- Ensure that change feed is not already enabled before proceeding.
		IF EXISTS(SELECT * FROM sys.databases WHERE name = @db_name AND is_change_feed_enabled = 1)
		BEGIN
			-- If database is enabled now, another thread was attempting to enable change feed
			-- at the same time. Raise an informational error.
			COMMIT TRAN

			-- ERROR: SYNAPSE_LINK_CANT_ENABLE_SYNAPSE_LINK_ALREADY_ENABLED
			RAISERROR(22705, 10, 4, @db_name)
			RETURN 0
		END

		DECLARE @containment TINYINT
		DECLARE @is_published BIT
		DECLARE @is_merge_published bit
		DECLARE @is_distributor BIT
		DECLARE @is_cdc_enabled BIT
		DECLARE @delayed_durability INT
		DECLARE @is_change_feed_enabled BIT
		SELECT @containment = containment,
				@is_published = is_published,
				@is_merge_published = is_merge_published,
				@is_distributor = is_distributor,
				@is_cdc_enabled = is_cdc_enabled,
				@delayed_durability = delayed_durability,
				@is_change_feed_enabled = is_change_feed_enabled
			FROM sys.databases WHERE name = @db_name

		-- Verify that database is not contained.
		IF (@containment != 0)
		BEGIN
			-- ERROR: CHANGE_STREAMS_ON_CDB_NOT_SUPPORTED
			RAISERROR(23649, 16, 1, @db_name)
		END

		-- Verify that database is not publication database in transactional/snapshot/merge replication
		-- and that database is not distribution database.
		DECLARE @repl_msg_id INT
		IF (@is_published = 1 OR @is_merge_published = 1 OR @is_distributor = 1)
		BEGIN
			SET @repl_msg_id = (CASE 
				WHEN @is_published = 1 THEN 22764 -- PH_REPLICATION_PUBLISH
				WHEN @is_merge_published = 1 THEN 22765 -- PH_REPLICATION_MERGE_PUBLISH
				WHEN @is_distributor = 1 THEN 22763 -- PH_REPLICATION_DISTRIBUTOR
				END)

			-- ERROR: SYNAPSE_LINK_IS_NOT_SUPPORTED_WITH_REPLICATION
			RAISERROR(22762, 16, -1, 23601 /*PH_CHANGE_STREAMS*/, @db_name, @repl_msg_id)
		END

		-- Verify that database is not enabled for CDC.
		IF (@is_cdc_enabled = 1)
		BEGIN
			-- ERROR: CHANGE_STREAMS_CANT_ENABLE_WITH_CDC
			RAISERROR(23650, 16, 1, @db_name)
		END

		-- Verify that delayed durability is not set on the database.
		IF (@delayed_durability <> 0)
		BEGIN
			-- ERROR: CHANGE_STREAMS_CANT_ENABLE_WITH_DELAYED_DURABILITY
			RAISERROR(23651, 16, 1, @db_name)
		END

		-- Verify that database is not mirrored.
		IF EXISTS (SELECT * FROM sys.database_mirroring WHERE database_id = @db_id AND mirroring_state IS NOT NULL)
		BEGIN
			-- ERROR: CHANGE_STREAMS_CANT_ENABLE_WITH_MIRRORING
			RAISERROR(23652, 16, 1, @db_name)
		END

		-- Verify that change feed is not already enabled for the database.
		IF (@is_change_feed_enabled = 1)
		BEGIN
			SET @action = N'CheckIfChangeFeedIsEnabled'

			-- ERROR: SYNAPSE_LINK_CANT_ENABLE_SYNAPSE_LINK_ALREADY_ENABLED
			RAISERROR(22705, 16, 3, @db_name)
		END

		SET @logmessage = CONCAT(N'Successfully completed validation. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_enable_event_stream', @logmessage

		-- Removing (if present) temporary winfab property to skip scans on the db.
		SET @action = N'sys.sp_cloud_remove_skip_scan_property'
		EXEC sys.sp_cloud_remove_skip_scan_property

		-- Mark the database as enabled for Change Event Streaming.
		-- Set CES will also set the Synapse Link bit to 1 (is_change_feed_enabled).
		SET @action = N'SetCes(Value = 1)'
		EXEC %%DatabaseEx(Name = @db_name).SetCes(Value = 1)

		SET @logmessage = CONCAT(N'Marked the db for CES publishing. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_enable_event_stream', @logmessage

		-- With DB lock held as part of SetCes, double check if DB is enabled for tran repl and CDC in case
		-- a concurrent command has enabled it.
		IF EXISTS (SELECT * FROM sys.databases WHERE name = @db_name AND (is_published = 1 OR is_cdc_enabled = 1))
		BEGIN
			-- ERROR: CHANGE_STREAMS_CANT_ENABLE_WITH_CDC
			RAISERROR(23650, 16, 2, @db_name)
		END

		EXEC sys.sp_change_streams_check_to_use_internal_tables @check_to_use_internal_tables OUTPUT

		IF (@check_to_use_internal_tables = 1)
		BEGIN
			-- Create internal tables.
			SET @action = N'sp_trident_link_create_internal_tables'
			EXEC sp_trident_link_create_internal_tables

			INSERT INTO [sys].[change_feed_settings] (maxtrans, seqno ,schema_version, pollinterval, reseed_state)
				VALUES (@maxtrans, @seqno, @schema_version, @pollinterval, 0)

			SET @logmessage = CONCAT(N'Created internal tables for db that is enabled for Change Event Streaming. DB ID: ', @db_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_enable_event_stream', @logmessage
		END
		ELSE
		BEGIN
			-- Switch to 'dbo' before creating the changefeed schema and user.
			-- This is the 'dbo' user of the database to be enabled,
			-- not the 'dbo' of the resource database.
			EXECUTE AS USER = 'dbo'
			SET @swuser_flag = 1

			-- Create database changefeed user and mark this user created by this sproc.
			SET @action = N'create user changefeed'
			CREATE USER [changefeed] WITHOUT LOGIN WITH DEFAULT_SCHEMA = [changefeed]
			SET @logmessage = CONCAT(N'Created changefeed user. DB ID: ', @db_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_enable_event_stream', @logmessage

			-- Grant database control permission to user 'changefeed'
			SET @action = N'GRANT CONTROL ON DATABASE to user changefeed'
			DECLARE @sql NVARCHAR(MAX)
			SET @sql = N'GRANT CONTROL ON DATABASE::' +  QUOTENAME(@db_name) + N' TO [changefeed]'
			EXEC sp_executesql @sql
			SET @logmessage = CONCAT(N'Granted database control permission to user ''changefeed''. DB ID: ', @db_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_enable_event_stream', @logmessage

			REVERT
			SET @swuser_flag = 0

			-- Switch to changefeed user to create the remaining objects.
			EXECUTE AS USER = 'changefeed'
			SET @swuser_flag = 1

			-- Create tables under changefeed schema that will be used by CES.
			SET @action = N'sys.sp_MSchange_feed_create_objects'
			EXEC [sys].[sp_MSchange_feed_create_objects]
			SET @logmessage = CONCAT(N'Completed sp_MSchange_feed_create_objects. DB ID: ', @db_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_enable_event_stream', @logmessage

			INSERT INTO [changefeed].[change_feed_settings] (maxtrans, pollinterval) VALUES (@maxtrans, @pollinterval)
			SET @logmessage = CONCAT(N'Inserted entry into change_feed_settings. DB ID: ', @db_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_enable_event_stream', @logmessage

			REVERT
			SET @swuser_flag = 0
		END

		COMMIT TRAN

		SET @logmessage = CONCAT(N'Committed transaction inside the TRY block. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_enable_event_stream', @logmessage
	END TRY

	BEGIN CATCH
		SET @logmessage = CONCAT(N'Entering CATCH block. DB ID: ', @db_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_enable_event_stream', @logmessage

		IF (XACT_STATE() = -1)
		BEGIN
			ROLLBACK TRAN
			SET @logmessage = concat(N'Transaction rolled back. DB ID: ', @db_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_enable_event_stream', @logmessage
		END
		ELSE IF (XACT_STATE() = 1)
		BEGIN
			ROLLBACK TRAN xact_sp_enable_event_stream
			COMMIT TRAN
			SET @logmessage = CONCAT(N'Transaction rolled back to savepoint: xact_sp_enable_event_stream. DB ID: ', @db_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_enable_event_stream', @logmessage
		END

		-- Save the error number and associated message raised in the TRY block.
		DECLARE @raised_error INT, @raised_state INT, @raised_message NVARCHAR(4000)
		SELECT @raised_error = ERROR_NUMBER()
		SELECT @raised_state = ERROR_STATE()
		SELECT @raised_message = ERROR_MESSAGE()

		IF (@check_to_use_internal_tables = 0 AND DATABASE_PRINCIPAL_ID ('changefeed') IS NOT NULL)
		BEGIN
			IF (@swuser_flag = 1)
			BEGIN
				REVERT
				SET @swuser_flag = 0
			END

			EXECUTE AS USER = 'dbo'
			SET @swuser_flag = 1

			SET @action = @action + N' Drop user [changefeed] if it exists'
			DROP USER changefeed
			SET @logmessage = CONCAT(N'Dropped changefeed user. DB ID: ', @db_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_enable_event_stream', @logmessage
			REVERT
			SET @swuser_flag = 0
		END

		IF (@swuser_flag = 1)
		BEGIN
			REVERT
		END

		SET @logmessage = CONCAT(N'Caught an exception while performing action: ', @action,
			N'. DB ID: ', @db_id, 
			N'. Error code: ', @raised_error, 
			N'. Error state: ', @raised_state)
		EXEC sys.sp_synapse_link_fire_trace_event @raised_error, N'sp_enable_event_stream', @logmessage

		-- ERROR: CHANGE_STREAMS_STORED_PROC_FAILED
		RAISERROR (23626, 16, 5, @raised_error, @raised_state, @raised_message)

		RETURN 1
	END CATCH

	RETURN 0
END

