use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_cdc_help_jobs
as
begin

    declare @retcode int

    --
    -- Job security check proc
    --
    exec @retcode = [sys].[sp_MScdc_job_security_check]
    if @retcode <> 0 or @@error <> 0
        return(1)
        
    -- Verify database is currently enabled for change data capture
    if ([sys].[fn_cdc_is_db_enabled]() != 1)
    begin
		declare @db_name sysname
		set @db_name = db_name()
		raiserror(22901, 16, -1, @db_name)
        return 1
    end
        
	if (serverproperty('EngineEdition') not in (5,12))
	begin
	exec @retcode = sys.sp_cdc_help_jobs_internal
	end
	else
	begin
		exec @retcode = sys.sp_cdc_help_jobs_internal_db_scoped
	end
	
	if @@error <> 0 or @retcode <> 0
		return 1
		
	return 0
end		


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_cdc_help_jobs
as
begin

    declare @retcode int

    --
    -- Job security check proc
    --
    exec @retcode = [sys].[sp_MScdc_job_security_check]
    if @retcode <> 0 or @@error <> 0
        return(1)
        
    -- Verify database is currently enabled for change data capture
    if ([sys].[fn_cdc_is_db_enabled]() != 1)
    begin
		declare @db_name sysname
		set @db_name = db_name()
		raiserror(22901, 16, -1, @db_name)
        return 1
    end
        
	if (serverproperty('EngineEdition') <> 5)
	begin
	exec @retcode = sys.sp_cdc_help_jobs_internal
	end
	else
	begin
		exec @retcode = sys.sp_cdc_help_jobs_internal_db_scoped
	end
	
	if @@error <> 0 or @retcode <> 0
		return 1
		
	return 0
end		

