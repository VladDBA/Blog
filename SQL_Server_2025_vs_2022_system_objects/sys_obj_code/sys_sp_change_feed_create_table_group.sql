use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
CREATE PROCEDURE sys.[sp_change_feed_create_table_group]
(
	@table_group_id uniqueidentifier,
	@table_group_name nvarchar(140),
	@workspace_id nvarchar(247) = NULL,
	@destination_location nvarchar(512) = NULL,
	@destination_credential sysname = NULL
)
as
BEGIN
	declare @retcode int
		,@logmessage nvarchar(4000)

	declare @destination_type int
	-- Trident Link :  set destination_type as 2 if both is_change_feed_enabled and is_data_lake_replication_enabled are set to 1 in sys.databases
	-- Synapse Link : set destination_type as 0 if only is_change_feed_enabled is set to 1 in sys.databases
	-- We set these columns when we call enable db , so we can leverage this information to set  at the create table group level
	-- This does not need to use the fn_trident_link_is_enabled_for_current_db because this should never be called directly on the sql Azure instance
	
	set @destination_type = ISNULL((select 2 from sys.databases where (database_id = db_id()
	-- Adding support for azure (EngineEdition = 5), azure SQL MI (EngineEdition = 8) and Trident Native Sql DB (EngineEdition = 12)
	or (name = db_name() and serverproperty('EngineEdition') in (5, 8, 12)))
	and is_change_feed_enabled = 1 and is_data_lake_replication_enabled = 1), 0)
	
	set @logmessage = concat(N'Executing internal proc. Table Group ID: ', @table_group_id)
	exec sys.sp_synapse_link_fire_trace_event 0, N'sp_change_feed_create_table_group', @logmessage
	
	exec @retcode = sys.sp_change_feed_create_table_group_internal @table_group_id, @table_group_name, @workspace_id, @destination_location, @destination_credential, @destination_type
	
	declare @errcode int = @@error
	declare @status int = case when @errcode <> 0 then @errcode when @retcode <> 0 then @retcode else 0 end
	
	set @logmessage = concat(N'Completed internal proc. Table Group ID: ', @table_group_id,
		N'. Return code: ', @retcode, 
		N'. Error code: ', @errcode)
	exec sys.sp_synapse_link_fire_trace_event @status, N'sp_change_feed_create_table_group', @logmessage
	
	return @status
END


/*====  SQL Server 2022 version  ====*/
CREATE PROCEDURE sys.[sp_change_feed_create_table_group]
(
	@table_group_id uniqueidentifier,
	@table_group_name nvarchar(140),
	@workspace_id nvarchar(247),
	@destination_location nvarchar(512) = NULL,
	@destination_credential sysname = NULL
)
as
BEGIN
	declare @base_lz_location nvarchar(512)
			,@resource nvarchar(255)
			,@applock_result int
			,@db_name sysname
			,@db_id int
			,@swuser_flag bit
			,@raised_error int
			,@raised_state int
			,@raised_message nvarchar(4000)
			,@action nvarchar(1000)
			,@trancount int
			,@supported bit
			,@allowCredential bit
			,@synapse_workgroup_name nvarchar(50)
			,@tableGroupCount int

	set nocount on

	set @db_name = db_name()
	set @db_id = db_id()
	set @raised_error = 0
	set @swuser_flag = 0
	set @resource = N'__$synapse_link__db_' + convert(nvarchar(10),@db_id)
	
	-- Verify Synapse Link is supported for this server
	set @supported = [sys].[fn_synapse_link_is_supported]()
	IF (@@ERROR <> 0 or @supported = 0)
	BEGIN
		RAISERROR(22701, 16, 5)
		RETURN (1)
	END

	IF (sys.fn_has_permission_run_changefeed() = 0)
	BEGIN
		RAISERROR(22702, 16, 1)
		RETURN (1)
	END

	-- This does not need to use the fn_change_feed_is_enabled_for_current_db because this should never be called directly on the sql Azure instance
	if not exists (select * from sys.databases where name = @db_name and is_change_feed_enabled = 1)
	begin
		raiserror(22706, 16, 1, @db_name)
		return 1
	end
	
	if ([sys].[fn_synapse_link_table_group_limit_feature_switch]() = 1)
	begin
		select @tableGroupCount = count(*) from changefeed.[change_feed_table_groups]
		if (@tableGroupCount >= 4096)
		begin
			raiserror(22787, 16, 1, 4096)
			return 1
		end
	end

	declare @vupgraderesult int
	exec @vupgraderesult = [sys].[sp_change_feed_vupgrade]
	if (@vupgraderesult != 0)
	begin
		return 1
	end

	if (sys.fn_synapse_link_is_sas_from_credential_allowed() = 1)
	begin
		if (@destination_location is null) or (@destination_location = N'') 
		begin
			raiserror(22709, 16, -1, N'@destination_location')
			return 1
		end

		-- For non SQL DB flavors and xcopy, destination_credential is not required.
		-- destination_credential can be parsed from the destination_location.
		if ((SERVERPROPERTY('IsXCopyInstance') IS NOT NULL) OR (SERVERPROPERTY('EngineEdition') <> 5))
		begin
			-- if destination_credential is not passed , extract destination_credential from destination_location
			if (@destination_credential is null) or (@destination_credential = N'')
			begin
				set @destination_credential = sys.fn_synapse_link_getContainerURL(@destination_location)
			end
		end

		if (@destination_credential is null) or (@destination_credential = N'') 
		begin
			raiserror(22709, 16, -1, N'@destination_credential')
			return 1
		end
		
		if not exists (select * FROM sys.database_scoped_credentials
		where name = @destination_credential COLLATE SQL_Latin1_General_CP1_CI_AS
			AND credential_identity = 'SHARED ACCESS SIGNATURE' COLLATE SQL_Latin1_General_CP1_CI_AS)
		begin
			raiserror(22707, 16, 1)
			return 1
		end

		declare @retxstore int = 0
		exec @retxstore = sys.sp_synapse_link_is_xstore_path @destination_location
		if (@retxstore != 0)
		begin
			raiserror(22740, 16, 3, N'@destination_location')
			return 1
		end

		select @base_lz_location = sys.fn_synapse_link_getContainerURL(@destination_location)
		if (@base_lz_location = N'' or @base_lz_location <> lower(@destination_credential COLLATE SQL_Latin1_General_CP1_CI_AS))
		begin
			raiserror(22708, 16, 1)
			return 1
		end
	end
	else
	begin
		if @destination_location IS NOT NULL OR @destination_credential IS NOT NULL
		BEGIN
			raiserror(22741, 16, 1)
			return 1
		END
	end

	if (sys.fn_change_feed_is_valid_table_group_name(@table_group_name) = 0)
	begin
		raiserror(22740, 16, 1, N'@table_group_name')
		return 1
	end

	select @synapse_workgroup_name = sys.fn_synapse_link_extract_workspace_name(@workspace_id)
	if @synapse_workgroup_name = N''
	begin
		raiserror(22740, 16, 2, N'@workspace_id')
		return 1
	end

	-- Check the OFR for SQLDB
	if serverproperty('EngineEdition') = 5
	BEGIN
		declare @retWorkGroupName int = 0
		exec @retWorkGroupName = sys.sp_synapse_link_validate_ofr @synapse_workgroup_name
		if (@retWorkGroupName != 0)
		begin
			raiserror(22786, 16, 1)
		end
	END

	BEGIN TRY
		set @trancount = @@trancount

		begin TRAN
		save tran sp_synapse_link_create_topic

		--  Get Shared database lock
		set @action = N'sys.sp_getapplock' 
		exec @applock_result = sys.sp_getapplock @Resource = @resource, @LockMode = N'Shared',
			@LockOwner = 'Transaction', @DbPrincipal =  'public'

		If @applock_result < 0
		begin
			-- Lock request failed.
			set @action = N'sys.sp_getapplock @Resource = ' + @resource + N'@LockMode = N''Shared'', @LockOwner = ''Transaction'', @DbPrincipal = ''public'' '
			raiserror(22712, 16, -1, @action, @applock_result)
		end

		-- Switch to database user 'changefeed' before executing stored procedure that
		-- can cause database DML triggers to fire.
		set @action = N'execute as user' 
		execute as user = 'changefeed'
		set @swuser_flag = 1

		set @action = N'insert into [changefeed].[change_feed_table_groups]'
		insert into [changefeed].[change_feed_table_groups] (table_group_id, table_group_name, destination_location,
				destination_credential, workspace_id, synapse_workgroup_name, enabled)
		values(@table_group_id, @table_group_name, @destination_location, @destination_credential,
				@workspace_id, @synapse_workgroup_name, 1)

		revert
		set @swuser_flag = 0

		commit tran
	END TRY
	BEGIN CATCH
		if @@trancount > @trancount
		begin
			-- If Synapse Link opened the transaction or it is not possible 
			-- to rollback to the savepoint, rollback the transaction
			if ( @trancount = 0 ) OR ( XACT_STATE() <> 1 )
			begin
				rollback tran 
			end
			-- Otherwise rollback to the savepoint
			else
			begin
				rollback tran sp_synapse_link_create_topic
				commit tran
			end
		end

		-- Save the error number and associated message raised in the TRY block
		select @raised_error = ERROR_NUMBER()
		select @raised_state = ERROR_STATE()
		select @raised_message = ERROR_MESSAGE()
		
		if @swuser_flag = 1
		begin
			revert
		end

		raiserror(22710, 16, -1, @action, @raised_error, @raised_state, @raised_message)  
		return 1
	END CATCH

	return 0
END

