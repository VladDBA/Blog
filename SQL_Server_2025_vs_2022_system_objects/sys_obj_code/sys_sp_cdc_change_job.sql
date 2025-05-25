SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_cdc_change_job
(
	@job_type nvarchar(20) = N'capture',
	@maxtrans int = null,
	@maxscans int = null,
	@continuous bit = null,
	@pollinginterval bigint = null,
	@retention bigint = null,
	@threshold bigint = null
)	
as
begin
    set nocount on

    declare @retcode int
		,@db_name sysname
		
	set @db_name = db_name()	

    --
    -- Job security check proc
    --
    exec @retcode = [sys].[sp_MScdc_job_security_check]
    if @retcode <> 0 or @@error <> 0
        return(1)
        
    -- Verify database is currently enabled for change data capture
    if ([sys].[fn_cdc_is_db_enabled]() != 1)
    begin
		raiserror(22901, 16, -1, @db_name)
        return 1
    end
        
	set @job_type = rtrim(ltrim(lower(@job_type)))
        
    -- Verify parameter
    if @job_type not in (N'capture', N'cleanup')
    begin
        raiserror(22992, 16, -1, @job_type)
        return(1)
    end
   
	if (serverproperty('EngineEdition') not in (5,12))
	begin
	-- Call internal stored procedure to complete verification and update job attributes
	exec @retcode = sys.sp_cdc_change_job_internal
		@job_type,
		@maxtrans,
		@maxscans,
		@continuous,
		@pollinginterval,
		@retention,
		@threshold
	end
	else
	begin
		-- Call internal stored procedure to complete verification and update job attributes
		exec @retcode = sys.sp_cdc_change_job_internal_db_scoped
			@job_type,
			@maxtrans,
			@maxscans,
			@continuous,
			@pollinginterval,
			@retention,
			@threshold
	end
		
	if @@error <> 0 or @retcode <> 0
		return 1

	-- Only on MI for now
	if (serverproperty('EngineEdition') = 8)
	begin
		begin try
			declare @job_parameters nvarchar(250)
			select @job_parameters = N'maxtrans: ' + isnull(cast(@maxtrans as nvarchar(10)), 'null') + N'; maxscans: ' + isnull(cast(@maxscans as nvarchar(10)), 'null') +
				N'; continuous: ' + isnull(cast(@continuous as nvarchar(5)), 'null') + N'; pollinginterval: ' + isnull(cast(@pollinginterval as nvarchar(20)), 'null') +
				N'; retention: ' + isnull(cast(@retention as nvarchar(20)), 'null') + N'; threshold: ' + isnull(cast(@threshold as nvarchar(20)), 'null')

			exec sp_cdc_generate_job_event
				@event = N'cdc_job',
				@event_type = N'change_job',
				@job_type = @job_type,
				@job_parameters = @job_parameters,
				@error_message = N'';
		end try
		begin catch
			-- No-op for now.
		end catch
	end

	return 0
end


/*====  SQL Server 2022 version  ====*/
create procedure sys.sp_cdc_change_job
(
	@job_type nvarchar(20) = N'capture',
	@maxtrans int = null,
	@maxscans int = null,
	@continuous bit = null,
	@pollinginterval bigint = null,
	@retention bigint = null,
	@threshold bigint = null
)	
as
begin
    set nocount on

    declare @retcode int
		,@db_name sysname
		
	set @db_name = db_name()	

    --
    -- Job security check proc
    --
    exec @retcode = [sys].[sp_MScdc_job_security_check]
    if @retcode <> 0 or @@error <> 0
        return(1)
        
    -- Verify database is currently enabled for change data capture
    if ([sys].[fn_cdc_is_db_enabled]() != 1)
    begin
		raiserror(22901, 16, -1, @db_name)
        return 1
    end
        
	set @job_type = rtrim(ltrim(lower(@job_type)))
        
    -- Verify parameter
    if @job_type not in (N'capture', N'cleanup')
    begin
        raiserror(22992, 16, -1, @job_type)
        return(1)
    end
   
	if (serverproperty('EngineEdition') <> 5)
	begin
	-- Call internal stored procedure to complete verification and update job attributes
	exec @retcode = sys.sp_cdc_change_job_internal
		@job_type,
		@maxtrans,
		@maxscans,
		@continuous,
		@pollinginterval,
		@retention,
		@threshold
	end
	else
	begin
		-- Call internal stored procedure to complete verification and update job attributes
		exec @retcode = sys.sp_cdc_change_job_internal_db_scoped
			@job_type,
			@maxtrans,
			@maxscans,
			@continuous,
			@pollinginterval,
			@retention,
			@threshold
	end
		
	if @@error <> 0 or @retcode <> 0
		return 1

	-- Only on MI for now
	if (serverproperty('EngineEdition') = 8)
	begin
		begin try
			declare @job_parameters nvarchar(250)
			select @job_parameters = N'maxtrans: ' + isnull(cast(@maxtrans as nvarchar(10)), 'null') + N'; maxscans: ' + isnull(cast(@maxscans as nvarchar(10)), 'null') +
				N'; continuous: ' + isnull(cast(@continuous as nvarchar(5)), 'null') + N'; pollinginterval: ' + isnull(cast(@pollinginterval as nvarchar(20)), 'null') +
				N'; retention: ' + isnull(cast(@retention as nvarchar(20)), 'null') + N'; threshold: ' + isnull(cast(@threshold as nvarchar(20)), 'null')

			exec sp_cdc_generate_job_event
				@event = N'cdc_job',
				@event_type = N'change_job',
				@job_type = @job_type,
				@job_parameters = @job_parameters,
				@error_message = N'';
		end try
		begin catch
			-- No-op for now.
		end catch
	end

	return 0
end

