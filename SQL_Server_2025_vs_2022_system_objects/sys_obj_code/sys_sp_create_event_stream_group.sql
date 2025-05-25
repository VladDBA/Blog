use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

CREATE   PROCEDURE sys.sp_create_event_stream_group
	@stream_group_name SYSNAME,
	@destination_type SYSNAME,
	@destination_location NVARCHAR(4000), 
	@destination_credential SYSNAME, 
	@max_message_size_bytes INT = 262144, -- 256KB
	@partition_key_scheme SYSNAME = N'None',
	@partition_key_column_name SYSNAME = NULL,
	@encoding SYSNAME = N'JSON'
AS
BEGIN
	SET NOCOUNT ON
	SET XACT_ABORT ON

	DECLARE @action NVARCHAR(1000)
	DECLARE @errorMessage NVARCHAR(max)
	DECLARE @logmessage NVARCHAR(4000)
	DECLARE @db_name SYSNAME
	DECLARE @db_id INT
	DECLARE @swuser_flag BIT
	DECLARE @stream_group_id UNIQUEIDENTIFIER

	SET @db_name = DB_NAME()
	SET @db_id = DB_ID()
	SET @swuser_flag = 0
	SET @stream_group_id = NEWID()

	BEGIN TRY
		SET @action = N'Validating parameters'
		SET @logmessage = CONCAT(N'Starting parameter validation. Stream Group ID: ', @stream_group_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_create_event_stream_group', @logmessage

		-- Checks permissions and validates that CES is enabled.
		EXEC sys.sp_change_streams_validation

		-- Change Streams has certain constraints on the parameters
		-- that change feed may not have. Therefore we need to validate
		-- the parameters based on change streams requirements.

		-- 1.) Verify that stream_group_name is not null or empty.
		IF (@stream_group_name IS NULL) OR (@stream_group_name = N'')
		BEGIN
			-- ERROR: CHANGE_STREAMS_INVALID_ARGUMENT
			RAISERROR (23625, 16, 5, N'@stream_group_name')
		END

		-- 2.) Verify the partition_key_scheme is correct and supported.
		DECLARE @partition_scheme_id INT
		IF (@partition_key_scheme = N'None')
		BEGIN
			SET @partition_scheme_id = 0
		END
		ELSE IF (@partition_key_scheme = N'StreamGroup')
		BEGIN
			SET @partition_scheme_id = 1
		END
		ELSE IF (@partition_key_scheme = N'Table')
		BEGIN
			SET @partition_scheme_id = 2
		END
		ELSE IF (@partition_key_scheme = N'Column')
		BEGIN
			SET @partition_scheme_id = 3
		END
		ELSE
		BEGIN
			-- ERROR: CHANGE_STREAMS_INVALID_ARGUMENT
			RAISERROR (23625, 16, 8, N'@partition_key_scheme')
		END

		-- 3.) Verify destination_location parameter.
		--     ChangeStreamsEventHubsEmulator should only be ON when running tests. It allows the use of Event Hubs Emulator.
		DECLARE @is_change_streams_event_hubs_emulator_fs_enabled BIT = 0
		EXEC sys.sp_is_feature_enabled N'ChangeStreamsEventHubsEmulator',
									16702 /*TRCFLG_TEMP_CHANGE_STREAMS_ENABLE_EH_EMULATOR*/,
									1 /*enable TF*/,
									@is_change_streams_event_hubs_emulator_fs_enabled OUTPUT

		IF (@is_change_streams_event_hubs_emulator_fs_enabled = 0)
		BEGIN
			-- If ChangeStreamsEventHubsEmulator is OFF, we are in a production scenario.
			-- This check is based on ChangeFeedUrlHelpers::ParseEventHubUrl.
			-- If Event Hub URL parsing fails we will not populate EHNamespace and EHName. This will cause CES publishing to fail when publishing messages to Event Hubs.
			-- Instead of failing late on publishing, we are adding this check here so that table group is not created.
			-- The check enforces the following:
			--     - Contains .servicebus.windows.net/ or .servicebus.windows.net:<port_number>/
			--     - Does NOT contain protocol separator '://'. Starting with https:// used to be enforced but makes no sense, so the reverse is enforced now
			--     - Has exactly one forward slash separating the EH Host name and EH instance name
			--     - Has at least one character for EH Namespace
			--     - In case when there is colon after .servicebus.windows.net, we expect only port number (0-65535) after the colon and before the forward slash
			--     - Has at least one character for EH Instance
			-- We also validate that namespace and instance name follow the EH naming convention:
			--     - Namespace can only contain letters, numbers, and hyphens. Must start with a letter, must end with a letter or number
			--     - Instance name can only contain letters, numbers, period, hyphens, and underscores. Must start with a letter or number
			IF 
			(
				-- We expect only one of following formats:
				-- 1) namespace.servicebus.windows.net/instance_name
				-- 2) namespace.servicebus.windows.net:<port_number>/instance name
				(@destination_location NOT LIKE '%.servicebus.windows.net/%' AND @destination_location NOT LIKE '%.servicebus.windows.net:%/%') OR
				-- Check if the namespace contains invalid characters (part before .servicebus.windows.net)
				@destination_location LIKE '%[^a-zA-Z0-9-]%.servicebus.windows.net%' OR
				-- Check if the instance name contains invalid characters (part after .servicebus.windows.net%/)
				@destination_location LIKE '%.servicebus.windows.net%/%[^a-zA-Z0-9._-]%' OR
				-- Check if namespace does not start with a letter; or namespace does not end with a letter or number; or instance name does not start with a letter
				@destination_location NOT LIKE '[a-zA-Z]%[a-zA-Z0-9].servicebus.windows.net%/[a-zA-Z0-9]%'
			)
			BEGIN
				-- ERROR: CHANGE_STREAMS_INCORRECT_EVENT_HUBS_URL
				RAISERROR(23642, 16, 1)
			END

			IF 
			(
				 -- In case when there is colon after .servicebus.windows.net, we expect only port number after the colon and before the forward slash
				 -- Something like this '%.servicebus.windows.net:9093/%'
				@destination_location LIKE '%.servicebus.windows.net:%/%' AND 
				(
					-- Check if there are invalid characters (numbers are only allowed) after the colon before the forward slash
					@destination_location LIKE '%.net:%[^0-9]%/%' OR 
					-- Check if there is no character after the colon and before the forward slash (there must be at least one number character)
					@destination_location NOT LIKE '%.net:_%/%' OR
					-- Check if the port number is not between 0 and 65535
					TRY_CAST(SUBSTRING(
						@destination_location,
						CHARINDEX('.servicebus.windows.net:', @destination_location) + LEN('.servicebus.windows.net:'),
						CHARINDEX('/', @destination_location, CHARINDEX('.servicebus.windows.net:', @destination_location)) - 
						(CHARINDEX('.servicebus.windows.net:', @destination_location) + LEN('.servicebus.windows.net:'))
					) AS INT) NOT BETWEEN 0 AND 65535
				)
			)
			BEGIN
				-- ERROR: CHANGE_STREAMS_INCORRECT_PORT_FORMAT_IN_EVENT_HUBS_URL
				RAISERROR(23645, 16, 1)
			END
		END
		ELSE
		BEGIN
			-- If we are testing, ensure that there is only one forward slash
			IF (@destination_location NOT LIKE N'_%/_%')
			BEGIN
				-- ERROR: CHANGE_STREAMS_INVALID_ARGUMENT
				RAISERROR (23625, 16, 10, N'@destination_location')
			END
			IF (@destination_credential != N'EHEmulator')
			BEGIN
				-- ERROR: CHANGE_STREAMS_INVALID_ARGUMENT
				RAISERROR (23625, 16, 11, N'@destination_credential')
			END
		END

		-- 4.) Verify the encoding is correct and supported.
		DECLARE @encoding_id INT
		IF (@encoding = N'JSON')
		BEGIN
			SET @encoding_id = 1
		END
		ELSE IF (@encoding = N'Binary')
		BEGIN
			SET @encoding_id = 2
		END
		ELSE
		BEGIN
			-- ERROR: CHANGE_STREAMS_INVALID_ARGUMENT
			RAISERROR (23625, 16, 12, N'@encoding')
		END

		-- 5.) Verify max_message_size_bytes parameter is in the range [128KB, 1MB].
		IF (@max_message_size_bytes < 128 * 1024) OR (@max_message_size_bytes > 1024 * 1024)
		BEGIN
			-- ERROR: CHANGE_STREAMS_INVALID_ARGUMENT_DIFFERENT_VALUE_EXPECTED
			RAISERROR (23641, 16, 3, 'max_message_size_bytes', 'max_message_size_bytes', 'between 131072 and 1048576 bytes, inclusive')
		END

		-- 6.) Verify the destination_type is correct and supported.
		DECLARE @is_change_streams_kafka_protocol_fs_enabled BIT = 0
		EXEC sys.sp_is_feature_enabled N'ChangeStreamsKafkaProtocol',
									16704 /*TRCFLG_RETAIL_CHANGE_STREAMS_DISABLE_KAFKA_PROTOCOL*/,
									0 /*disable TF*/,
									@is_change_streams_kafka_protocol_fs_enabled OUTPUT

		IF (@is_change_streams_kafka_protocol_fs_enabled = 0)
		BEGIN
			-- When ChangeStreamsKafkaProtocol FS is set to OFF customer can only set AzureEventHubsAmqp as event destination.
			IF @destination_type != N'AzureEventHubsAmqp'
			BEGIN
				-- ERROR: CHANGE_STREAMS_INVALID_ARGUMENT
				RAISERROR (23625, 16, 16, N'@destination_type')
			END
		END
		ELSE
		BEGIN
			IF @destination_type NOT IN (N'AzureEventHubsAmqp', N'AzureEventHubsApacheKafka')
			BEGIN
				-- ERROR: CHANGE_STREAMS_INVALID_ARGUMENT
				RAISERROR (23625, 16, 15, N'@destination_type')
			END
		END

		DECLARE @destination_type_id INT = 1

		DECLARE @workspace_id NVARCHAR(247)
		DECLARE @synapse_workgroup_name NVARCHAR(50)
		SET @synapse_workgroup_name = N'ChangeStreams'
		SET @workspace_id = N'changestreams'

		EXEC [sys].[sp_change_feed_vupgrade]

		-- Simulate a timeout if on a test path.
		EXEC [sys].[sp_simulate_timeout_on_test_path]

		BEGIN TRAN
		SAVE TRAN sp_change_streams_create_group

		--  Get Shared database lock.
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

		-- The lock request was granted.
		SET @logmessage = CONCAT(N'The lock request was granted. Stream Group ID: ', @stream_group_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_create_event_stream_group', @logmessage

		-- Checks permissions and validates that CES is enabled.
		EXEC sys.sp_change_streams_validation

		-- Verify that stream group with given stream_group_name does not already exist.
		DECLARE @check_to_use_internal_tables BIT
		EXEC sys.sp_change_streams_check_to_use_internal_tables @check_to_use_internal_tables OUTPUT

		IF (@check_to_use_internal_tables = 1)
		BEGIN
			IF EXISTS (
				SELECT *
				FROM sys.change_feed_table_groups
				WHERE table_group_name = @stream_group_name
					AND destination_type = 1 -- CES destination type
			)
			BEGIN
				-- ERROR: CHANGE_STREAMS_INVALID_ARGUMENT
				RAISERROR (23625, 16, 14, N'@stream_group_name')
			END
		END
		ELSE
		BEGIN
			IF EXISTS (
				SELECT *
				FROM changefeed.change_feed_table_groups
				WHERE table_group_name = @stream_group_name
					AND destination_type = 1 -- CES destination type
			)
			BEGIN
				-- ERROR: CHANGE_STREAMS_INVALID_ARGUMENT
				RAISERROR (23625, 16, 6, N'@stream_group_name')
			END
		END

		-- Verify the destination_credential parameter.
		DECLARE @is_change_feed_allow_sas_from_credential_fs_enabled BIT = 0
		EXEC sys.sp_is_feature_enabled N'ChangeFeedAllowSASFromCredential',
									13820 /*TRCFLG_RETAIL_CHANGE_FEED_DISABLE_SAS_FROM_CREDENTIAL*/,
									0 /*disable TF*/,
									@is_change_feed_allow_sas_from_credential_fs_enabled OUTPUT

		IF (@is_change_feed_allow_sas_from_credential_fs_enabled = 1)
		BEGIN
			IF (@destination_credential IS NULL) OR (@destination_credential = N'')
			BEGIN
				-- ERROR: CHANGE_STREAMS_INVALID_ARGUMENT
				RAISERROR (23625, 16, 17, N'@destination_credential')
			END

			IF NOT EXISTS (SELECT * FROM sys.database_scoped_credentials
							WHERE name = @destination_credential COLLATE SQL_Latin1_General_CP1_CI_AS
							AND credential_identity = 'SHARED ACCESS SIGNATURE' COLLATE SQL_Latin1_General_CP1_CI_AS)
			BEGIN
				-- ERROR: SYNAPSE_LINK_SYNAPSE_LINK_CREDENTIAL_NAME_FOUND
				RAISERROR(22707, 16, 2)
			END
		END

		-- Verify that limit of 4096 stream groups is not hit.
		DECLARE @stream_group_count INT
		IF (@check_to_use_internal_tables = 1)
		BEGIN
			SELECT @stream_group_count = COUNT(*) FROM sys.[change_feed_table_groups]
		END
		ELSE
		BEGIN
			SELECT @stream_group_count = COUNT(*) FROM changefeed.[change_feed_table_groups]
		END
		IF (@stream_group_count >= 4096)
		BEGIN
			-- ERROR: CHANGE_STREAMS_TABLE_GROUP_LIMIT_EXCEDED
			RAISERROR(23655, 16, 1, 4096)
		END

		SET @logmessage = CONCAT(N'Successfully completed parameter validation. Stream Group ID: ', @stream_group_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_create_event_stream_group', @logmessage

		IF (@check_to_use_internal_tables = 1)
		BEGIN
			SET @action = N'insert into [sys].[change_feed_table_groups]'
			DECLARE @stmt_to_exec nvarchar(max)
			SET @stmt_to_exec = N'
				insert into [sys].[change_feed_table_groups] (table_group_id, table_group_name, destination_location,
						destination_credential, workspace_id, synapse_workgroup_name, enabled, destination_type, max_message_size_bytes, partition_scheme, partition_column_name, encoding, streaming_dest_type)
				values(@stream_group_id, @stream_group_name, @destination_location, @destination_credential,
						@workspace_id, @synapse_workgroup_name, 1, @destination_type_id, @max_message_size_bytes, @partition_scheme_id, @partition_key_column_name, @encoding_id, @destination_type)'
			EXEC sp_executesql @stmt_to_exec,
								N'@stream_group_id uniqueidentifier,
									@stream_group_name nvarchar(140),
									@destination_location nvarchar(512),
									@destination_credential sysname,
									@workspace_id nvarchar(247),
									@synapse_workgroup_name nvarchar(50),
									@destination_type_id int,
									@max_message_size_bytes int,
									@partition_scheme_id int,
									@partition_key_column_name sysname,
									@encoding_id int,
									@destination_type sysname',
								@stream_group_id,
								@stream_group_name,
								@destination_location,
								@destination_credential,
								@workspace_id,
								@synapse_workgroup_name,
								@destination_type_id,
								@max_message_size_bytes,
								@partition_scheme_id,
								@partition_key_column_name,
								@encoding_id,
								@destination_type
		END
		ELSE
		BEGIN
			-- Switch to database user 'changefeed'.
			SET @action = N'execute as user' 
			EXECUTE AS USER = 'changefeed'
			SET @swuser_flag = 1
			SET @action = N'insert into [changefeed].[change_feed_table_groups]'
			INSERT INTO [changefeed].[change_feed_table_groups] (table_group_id,
																 table_group_name,
																 destination_location,
																 destination_credential,
																 workspace_id,
																 synapse_workgroup_name,
																 enabled,
																 destination_type,
																 max_message_size_bytes,
																 partition_scheme,
																 partition_column_name,
																 encoding,
																 streaming_dest_type)
			values(@stream_group_id,
				   @stream_group_name,
				   @destination_location,
				   @destination_credential,
				   @workspace_id,
				   @synapse_workgroup_name,
				   1,
				   @destination_type_id,
				   @max_message_size_bytes,
				   @partition_scheme_id,
				   @partition_key_column_name,
				   @encoding_id,
				   @destination_type)

			REVERT
			SET @swuser_flag = 0
		END

		SET @logmessage = CONCAT(N'Inserted an entry into change_feed_table_groups table. Stream Group ID: ', @stream_group_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_create_event_stream_group', @logmessage

		COMMIT TRAN

		SET @logmessage = CONCAT(N'Committed transaction inside the TRY block. Stream Group ID: ', @stream_group_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_create_event_stream_group', @logmessage
	END TRY

	BEGIN CATCH
		SET @logmessage = CONCAT(N'Entering CATCH block. Stream Group ID: ', @stream_group_id)
		EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_create_event_stream_group', @logmessage

		IF (XACT_STATE() = -1)
		BEGIN
			ROLLBACK TRAN
			SET @logmessage = concat(N'Transaction rolled back. Stream Group ID: ', @stream_group_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_create_event_stream_group', @logmessage
		END
		ELSE IF (XACT_STATE() = 1)
		BEGIN
			ROLLBACK TRAN sp_change_streams_create_group
			COMMIT TRAN
			SET @logmessage = CONCAT(N'Transaction rolled back to savepoint: sp_change_streams_create_group. Stream Group ID: ', @stream_group_id)
			EXEC sys.sp_synapse_link_fire_trace_event 0, N'sp_create_event_stream_group', @logmessage
		END

		-- Save the error number and associated message raised in the TRY block.
		DECLARE @raised_error INT, @raised_state INT, @raised_message NVARCHAR(4000)
		SELECT @raised_error = ERROR_NUMBER()
		SELECT @raised_state = ERROR_STATE()
		SELECT @raised_message = ERROR_MESSAGE()

		IF @swuser_flag = 1
		BEGIN
			REVERT
		END

		SET @logmessage = CONCAT(N'Caught an exception while performing action: ', @action,
			N'. Stream Group ID: ', @stream_group_id, 
			N'. Error code: ', @raised_error, 
			N'. Error State: ', @raised_state)
		exec sys.sp_synapse_link_fire_trace_event @raised_error, N'sp_create_event_stream_group', @logmessage

		-- ERROR: CHANGE_STREAMS_STORED_PROC_FAILED
		RAISERROR (23626, 16, 2, @raised_error, @raised_state, @raised_message)

		RETURN 1
	END CATCH

	RETURN 0
END

