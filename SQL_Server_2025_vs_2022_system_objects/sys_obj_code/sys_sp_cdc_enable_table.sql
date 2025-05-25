use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_cdc_enable_table 
(
	@source_schema sysname,
	@source_name sysname,
	@capture_instance sysname = null,
	@supports_net_changes bit = null,
	@role_name sysname,
	@index_name sysname = null,
	@captured_column_list nvarchar(max) = null,
	@filegroup_name sysname = null,
	@allow_partition_switch bit = 1,
	@enable_extended_ddl_handling bit = 0
)
as
begin
	declare @retcode int
		,@db_name sysname
        
    -- Verify CDC is supported for this SQL Server edition
    IF ([sys].[fn_cdc_is_supported]() = 1)
    BEGIN
		IF (serverproperty('EngineEdition') = 12)
		BEGIN
			RAISERROR(22607, 16, -1, 'Change Data Capture (CDC) is')
		END
		ELSE
		BEGIN
			DECLARE @edition sysname
			SELECT @edition = CONVERT(sysname, SERVERPROPERTY('Edition'))
			RAISERROR(22988, 16, -1, @edition)
		END
        RETURN (1)
    END
   
    -- Verify caller is entitled to enable change data capture for the table 
    if (isnull(is_srvrolemember('sysadmin'),0) = 0) and (isnull(is_member('db_owner'),0) = 0)
	begin
   		raiserror(22904, 16, -1)
		return 1
	end
	
    -- Verify database is enabled for change data capture before we switch to cdc 
    if ([sys].[fn_cdc_is_db_enabled]() != 1)
    begin
		set @db_name = db_name()
		raiserror(22901, 16, -1, @db_name)
        return 1
    end

    -- Verify @enable_extended_ddl_handling is used only when feature switch ChangeDataCaptureHandleDDL is enabled
    if (@enable_extended_ddl_handling = 1 and ([sys].[fn_cdc_handle_ddl_featureswitch_is_enabled]() <> 1))
    begin
		raiserror(22874, 16, -1)
		return 1
	end

	-- Switch to database user 'cdc' before executing stored procedure that
	-- can cause database DML and DDL triggers to fire.
	execute as user = 'cdc'

	exec sys.sp_cdc_fire_trace_event 0, -1, N'sp_cdc_enable_table', N'entering'

	-- Call internal stored procedure that executes as 'dbo' to do the work.	
	exec @retcode = sys.sp_cdc_enable_table_internal 
		@source_schema,
		@source_name,
		@capture_instance,
		@supports_net_changes,
		@role_name,
		@index_name,
		@captured_column_list,
		@filegroup_name,
		@allow_partition_switch,
		@enable_extended_ddl_handling

	declare @status int = @@error
	if @status = 0 set @status = @retcode
	exec sys.sp_cdc_fire_trace_event 0, @status, N'sp_cdc_enable_table', N'complete'

	if (@status <> 0)
	begin
		revert
		return 1
	end

	revert
	return 0
end


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_cdc_enable_table 
(
	@source_schema sysname,
	@source_name sysname,
	@capture_instance sysname = null,
	@supports_net_changes bit = null,
	@role_name sysname,
	@index_name sysname = null,
	@captured_column_list nvarchar(max) = null,
	@filegroup_name sysname = null,
	@allow_partition_switch bit = 1,
	@enable_extended_ddl_handling bit = 0
)
as
begin
	declare @retcode int
		,@db_name sysname
        
    -- Verify CDC is supported for this SQL Server edition
    IF ([sys].[fn_cdc_is_supported]() = 1)
    BEGIN
        DECLARE @edition sysname
        SELECT @edition = CONVERT(sysname, SERVERPROPERTY('Edition'))
        RAISERROR(22988, 16, -1, @edition)
        RETURN (1)
    END
   
    -- Verify caller is entitled to enable change data capture for the table 
    if (isnull(is_srvrolemember('sysadmin'),0) = 0) and (isnull(is_member('db_owner'),0) = 0)
	begin
   		raiserror(22904, 16, -1)
		return 1
	end
	
    -- Verify database is enabled for change data capture before we switch to cdc 
    if ([sys].[fn_cdc_is_db_enabled]() != 1)
    begin
		set @db_name = db_name()
		raiserror(22901, 16, -1, @db_name)
        return 1
    end

   -- Verify @enable_extended_ddl_handling is used only when feature switch ChangeDataCaptureHandleDDL is enabled
   if (@enable_extended_ddl_handling = 1 and ([sys].[fn_cdc_handle_ddl_featureswitch_is_enabled]() <> 1))
   begin
		raiserror(22874, 16, -1)
		return 1
	end
	
	-- Switch to database user 'cdc' before executing stored procedure that
	-- can cause database DML and DDL triggers to fire.
	execute as user = 'cdc'

	-- Call internal stored procedure that executes as 'dbo' to do the work.	
	exec @retcode = sys.sp_cdc_enable_table_internal 
		@source_schema,
		@source_name,
		@capture_instance,
		@supports_net_changes,
		@role_name,
		@index_name,
		@captured_column_list,
		@filegroup_name,
		@allow_partition_switch,
		@enable_extended_ddl_handling
		
	if (@@error <> 0) or (@retcode <> 0)
	begin
		revert
		return 1
	end
	
	revert
	return 0
end

