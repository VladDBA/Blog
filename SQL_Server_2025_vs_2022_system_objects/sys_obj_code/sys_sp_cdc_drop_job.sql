SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_cdc_drop_job
(
	@job_type nvarchar(20)
)	
as
begin
    set nocount on

    declare @retval int
    --
    -- Authorization check.
    --
    if (isnull(is_srvrolemember('sysadmin'),0) = 0) and (isnull(is_member('db_owner'),0) = 0)
    begin
        raiserror(22904, 16, -1)
        return(1)
    end

	-- NOTE:  The bit identifying a database as enabled for cdc is
	--        cleared before the jobs can be dropped, so only admin
	--        authorization is checked here.  If this changes, then
	--        job security can be checked.
	--
    -- CDC Job security check 
    --
    --exec @retcode = [sys].[sp_MScdc_job_security_check] 
    --if @retcode <> 0 or @@error <> 0
    --    return (1)
        
    set @job_type = rtrim(ltrim(lower(@job_type)))    
    
    -- Verify parameter
    if ((@job_type is null) or (@job_type not in (N'capture', N'cleanup')))
    begin
        raiserror(22992, 16, -1, @job_type)
        return(1)
    end

	-- Call internal stored procedure to drop the job
	if (serverproperty('EngineEdition') not in (5,12))
	begin
	exec @retval = sys.sp_cdc_drop_job_internal
		@job_type
	end
	else
	begin
		exec @retval = sys.sp_cdc_drop_job_internal_db_scoped
			@job_type
	end
	
	if @@error <> 0 or @retval <> 0
		return 1

	-- Only on MI for now
	if (serverproperty('EngineEdition') = 8)
	begin
		begin try
			exec sp_cdc_generate_job_event
				@event = N'cdc_job',
				@event_type = N'drop_job',
				@job_type = @job_type,
				@job_parameters = N'',
				@error_message = N'';
		end try
		begin catch
			-- No-op for now.
		end catch
	end

	return(0)
end


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_cdc_drop_job
(
	@job_type nvarchar(20)
)	
as
begin
    set nocount on

    declare @retval int
    --
    -- Authorization check.
    --
    if (isnull(is_srvrolemember('sysadmin'),0) = 0) and (isnull(is_member('db_owner'),0) = 0)
    begin
        raiserror(22904, 16, -1)
        return(1)
    end

	-- NOTE:  The bit identifying a database as enabled for cdc is
	--        cleared before the jobs can be dropped, so only admin
	--        authorization is checked here.  If this changes, then
	--        job security can be checked.
	--
    -- CDC Job security check 
    --
    --exec @retcode = [sys].[sp_MScdc_job_security_check] 
    --if @retcode <> 0 or @@error <> 0
    --    return (1)
        
    set @job_type = rtrim(ltrim(lower(@job_type)))    
    
    -- Verify parameter
    if ((@job_type is null) or (@job_type not in (N'capture', N'cleanup')))
    begin
        raiserror(22992, 16, -1, @job_type)
        return(1)
    end

	-- Call internal stored procedure to drop the job
	if (serverproperty('EngineEdition') <> 5)
	begin
	exec @retval = sys.sp_cdc_drop_job_internal
		@job_type
	end
	else
	begin
		exec @retval = sys.sp_cdc_drop_job_internal_db_scoped
			@job_type
	end
	
	if @@error <> 0 or @retval <> 0
		return 1

	-- Only on MI for now
	if (serverproperty('EngineEdition') = 8)
	begin
		begin try
			exec sp_cdc_generate_job_event
				@event = N'cdc_job',
				@event_type = N'drop_job',
				@job_type = @job_type,
				@job_parameters = N'',
				@error_message = N'';
		end try
		begin catch
			-- No-op for now.
		end catch
	end

	return(0)
end

