use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure [sys].[sp_MScdc_cleanup_job]
as
begin
	declare @retcode int
			,@db_name sysname
			,@retention bigint
			,@threshold bigint
	
    set nocount on
    
    set @db_name = db_name()

    -- Verify caller is authorized to clean up database change tables 
    if (isnull(is_srvrolemember('sysadmin'),0) = 0) and (isnull(is_member('db_owner'),0) = 0)
    begin
   		raiserror(22904, 16, -1)
        return(1)
    end

    -- Verify database is enabled for change data capture 
    if ([sys].[fn_cdc_is_db_enabled]() != 1)
    begin
		raiserror(22910, 16, -1, @db_name)
        return(1)
    end

	-- Determine the database retention time
	if (serverproperty('EngineEdition') not in (5,12))
	begin
    exec @retcode = sp_cdc_get_cleanup_retention @retention output, @threshold output
	end
	else
	begin
		exec @retcode = sp_cdc_get_cleanup_retention_db_scoped @retention output, @threshold output
	end

    if @retcode <> 0 or @@error <> 0
		return(1)

    -- If retention is negative or greater than 52594800 ( 100 years) fail
    if (@retention is null) or (@retention <= 0) or (@retention > 52594800)
    begin
		raiserror(22994, 16, -1)
		return(1)
	end
	
    -- If threshold is negative fail
    if (@threshold is null) or (@threshold <= 0) 
    begin
		raiserror(22850, 16, -1)
		return(1)
	end	
  
	-- Call internal stored procedure to do the work here.
	-- Switch to database 'cdc' user to mitigate against malicious DML triggers.
	execute as user = 'cdc'
	
	exec @retcode = sys.sp_cdc_cleanup_job_internal @retention, @threshold
    if @retcode <> 0 or @@error <> 0
    begin
		revert
		return(1)
	end
	
	revert	
    
    return(0)
end


/*====  SQL Server 2022 version  ====*/
create procedure [sys].[sp_MScdc_cleanup_job]
as
begin
	declare @retcode int
			,@db_name sysname
			,@retention bigint
			,@threshold bigint
	
    set nocount on
    
    set @db_name = db_name()

    -- Verify caller is authorized to clean up database change tables 
    if (isnull(is_srvrolemember('sysadmin'),0) = 0) and (isnull(is_member('db_owner'),0) = 0)
    begin
   		raiserror(22904, 16, -1)
        return(1)
    end

    -- Verify database is enabled for change data capture 
    if ([sys].[fn_cdc_is_db_enabled]() != 1)
    begin
		raiserror(22910, 16, -1, @db_name)
        return(1)
    end

	-- Determine the database retention time
	if (serverproperty('EngineEdition') <> 5)
	begin
    exec @retcode = sp_cdc_get_cleanup_retention @retention output, @threshold output
	end
	else
	begin
		exec @retcode = sp_cdc_get_cleanup_retention_db_scoped @retention output, @threshold output
	end

    if @retcode <> 0 or @@error <> 0
		return(1)

    -- If retention is negative or greater than 52594800 ( 100 years) fail
    if (@retention is null) or (@retention <= 0) or (@retention > 52594800)
    begin
		raiserror(22994, 16, -1)
		return(1)
	end
	
    -- If threshold is negative fail
    if (@threshold is null) or (@threshold <= 0) 
    begin
		raiserror(22850, 16, -1)
		return(1)
	end	
  
	-- Call internal stored procedure to do the work here.
	-- Switch to database 'cdc' user to mitigate against malicious DML triggers.
	execute as user = 'cdc'
	
	exec @retcode = sys.sp_cdc_cleanup_job_internal @retention, @threshold
    if @retcode <> 0 or @@error <> 0
    begin
		revert
		return(1)
	end
	
	revert	
    
    return(0)
end

