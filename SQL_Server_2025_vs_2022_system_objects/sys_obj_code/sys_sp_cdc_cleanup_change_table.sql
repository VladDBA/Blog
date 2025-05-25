use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure [sys].[sp_cdc_cleanup_change_table]
(
	@capture_instance sysname,
	@low_water_mark	binary(10),
	@threshold bigint = 4999,
	@fCleanupFailed bit = 0 output
)
as
begin
	declare @retcode int
			,@db_name sysname
			,@xstr1 nvarchar(22)
	
    set nocount on
    
    set @db_name = db_name()

    -- Verify caller is authorized to clean up change tracking  
    if (isnull(is_srvrolemember('sysadmin'),0) = 0) and (isnull(is_member('db_owner'),0) = 0)
    begin
   		raiserror(22904, 16, -1)
        return 1
    end
    
    -- Verify database is enabled for change tracking 
    if ([sys].[fn_cdc_is_db_enabled]() != 1)
    begin
		raiserror(22910, 16, -1, @db_name)
        return 1
    end
    
    -- Parameter @threshold must be positive
    if (@threshold <= 0)
    begin
  		raiserror(22850, 16, -1)
        return 1
    end

    -- If non-null, parameter @low_water_mark must appear as the start_lsn 
    -- value of a current entry in the cdc.lsn_time_mapping table.
    if (@low_water_mark is not null)
    begin
		if not exists
			( select start_lsn from cdc.lsn_time_mapping
			where start_lsn = @low_water_mark )
		begin
			set @xstr1 = upper(sys.fn_varbintohexstr(@low_water_mark))
			raiserror(22964, 16, -1, @xstr1)
			return 1
		end
	end		  

	-- Call internal stored procedure to do the work 
	-- Switch to database 'cdc' user to mitigate against malicious dbo triggers
	execute as user = 'cdc'
	   
    exec @retcode = sys.sp_cdc_cleanup_change_table_internal
    	@capture_instance,
		@low_water_mark,
		@threshold,
		@fCleanupFailed output
		
	if (@@error <> 0) or (@retcode <> 0)
	begin
		revert
        return 1
	end
	
	revert
	
	return 0		
end


/*====  SQL Server 2022 version  ====*/
create procedure [sys].[sp_cdc_cleanup_change_table]
(
	@capture_instance sysname,
	@low_water_mark	binary(10),
	@threshold bigint = 5000,
	@fCleanupFailed bit = 0 output
)
as
begin
	declare @retcode int
			,@db_name sysname
			,@xstr1 nvarchar(22)
	
    set nocount on
    
    set @db_name = db_name()

    -- Verify caller is authorized to clean up change tracking  
    if (isnull(is_srvrolemember('sysadmin'),0) = 0) and (isnull(is_member('db_owner'),0) = 0)
    begin
   		raiserror(22904, 16, -1)
        return 1
    end
    
    -- Verify database is enabled for change tracking 
    if ([sys].[fn_cdc_is_db_enabled]() != 1)
    begin
		raiserror(22910, 16, -1, @db_name)
        return 1
    end
    
    -- Parameter @threshold must be positive
    if (@threshold <= 0)
    begin
  		raiserror(22850, 16, -1)
        return 1
    end

    -- If non-null, parameter @low_water_mark must appear as the start_lsn 
    -- value of a current entry in the cdc.lsn_time_mapping table.
    if (@low_water_mark is not null)
    begin
		if not exists
			( select start_lsn from cdc.lsn_time_mapping
			where start_lsn = @low_water_mark )
		begin
			set @xstr1 = upper(sys.fn_varbintohexstr(@low_water_mark))
			raiserror(22964, 16, -1, @xstr1)
			return 1
		end
	end		  

	-- Call internal stored procedure to do the work 
	-- Switch to database 'cdc' user to mitigate against malicious dbo triggers
	execute as user = 'cdc'
	   
    exec @retcode = sys.sp_cdc_cleanup_change_table_internal
    	@capture_instance,
		@low_water_mark,
		@threshold,
		@fCleanupFailed output
		
	if (@@error <> 0) or (@retcode <> 0)
	begin
		revert
        return 1
	end
	
	revert
	
	return 0		
end

