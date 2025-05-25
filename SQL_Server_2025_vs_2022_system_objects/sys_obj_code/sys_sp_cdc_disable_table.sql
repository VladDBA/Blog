use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_cdc_disable_table 
(
	@source_schema sysname,
	@source_name sysname,
	@capture_instance sysname
)
as
begin
	declare @retcode int
		,@db_name sysname
   
    -- Verify caller is authorized to disable change data capture for the table.
    -- Caller must either be a member of the fixed sysadmin SQL Server role, or
    -- a member of the current database db_owner role. 
    if (isnull(is_srvrolemember('sysadmin'),0) = 0) and (isnull(is_member('db_owner'),0) = 0)
    begin
 		raiserror(22904, 16, -1)
		return 1
    end
    
    -- Verify database is enabled for change data capture before switching to the database 'cdc' user 
    if ([sys].[fn_cdc_is_db_enabled]() != 1)
    begin
		set @db_name = db_name()
		raiserror(22901, 16, -1, @db_name)
        return 1
    end
 
	-- Switch to the database 'cdc' user prior to calling module that can
	-- cause DML and/or DDL triggers to fire.   
	execute as user = 'cdc'

	exec sys.sp_cdc_fire_trace_event 0, -1, N'sp_cdc_disable_table', N'entering'

	-- Call internal stored procedure that executes as 'cdc' user to do the work
	exec @retcode = sys.sp_cdc_disable_table_internal
		@source_schema,
		@source_name,
		@capture_instance

	declare @status int = @@error
	if @status = 0 set @status = @retcode
	exec sys.sp_cdc_fire_trace_event 0, @status, N'sp_cdc_disable_table', N'complete'

	if (@status <> 0)
	begin
		revert
		return 1
	end

	revert
	return 0
end


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_cdc_disable_table 
(
	@source_schema sysname,
	@source_name sysname,
	@capture_instance sysname
)
as
begin
	declare @retcode int
		,@db_name sysname
   
    -- Verify caller is authorized to disable change data capture for the table.
    -- Caller must either be a member of the fixed sysadmin SQL Server role, or
    -- a member of the current database db_owner role. 
    if (isnull(is_srvrolemember('sysadmin'),0) = 0) and (isnull(is_member('db_owner'),0) = 0)
    begin
 		raiserror(22904, 16, -1)
		return 1
    end
    
    -- Verify database is enabled for change data capture before switching to the database 'cdc' user 
    if ([sys].[fn_cdc_is_db_enabled]() != 1)
    begin
		set @db_name = db_name()
		raiserror(22901, 16, -1, @db_name)
        return 1
    end
 
	-- Switch to the database 'cdc' user prior to calling module that can
	-- cause DML and/or DDL triggers to fire.   
	execute as user = 'cdc'
    
    -- Call internal stored procedure that executes as 'cdc' user to do the work
    exec @retcode = sys.sp_cdc_disable_table_internal
		@source_schema,
		@source_name,
		@capture_instance
		
	if (@@error <> 0) or (@retcode <> 0)
	begin
		revert
		return 1
	end
	
	revert
	return 0
end

