use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure sys.sp_cdc_add_job
(
    @job_type nvarchar(20),
    @start_job bit = 1,
    @maxtrans int = null,
    @maxscans int = null,
    @continuous bit = null,
    @pollinginterval bigint = null,
    @retention bigint = null,
    @threshold bigint = null,
	@check_for_logreader bit = 0
)
as
begin
    set nocount on

    declare @job_name sysname
           ,@retval   int
           ,@job_id   uniqueidentifier
           ,@old_job_id   uniqueidentifier
           ,@job_step_uid  uniqueidentifier
           ,@index    int
           ,@category_name sysname
           ,@command  nvarchar(1000)
           ,@server   sysname
           ,@databasename sysname
           ,@user     sysname
           ,@schedule_name sysname
           ,@database_id int
           ,@valid_job bit
           ,@description nvarchar(100)
           ,@step_name nvarchar(100)
			,@logreader_exists bit

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
    
	--
    -- CDC Job security check 
    --
    exec @retval = [sys].[sp_MScdc_job_security_check] 
    if @retval <> 0 or @@error <> 0
        return (1)
        
    -- Verify database is currently enabled for change data capture
    if ([sys].[fn_cdc_is_db_enabled]() != 1)
    begin
		set @databasename = db_name()
		raiserror(22901, 16, -1, @databasename)
        return 1
    end
            
	set @job_type = rtrim(ltrim(lower(@job_type)))

	--
    -- Parameter validation
    --
    
    -- job type must be either 1 or 2
    if ((@job_type is null) or (@job_type not in (N'capture', N'cleanup')))
	begin
		raiserror(22992, 16, -1, @job_type)
		return(1)
	end
	
	--
    -- Insure that transactional replication is not also enabled for the database
    --
    if (@job_type = N'capture')
    begin
		set @logreader_exists = 0
		exec @retval = [sys].[sp_MScdc_tranrepl_check] @logreader_exists = @logreader_exists output, @skip_remote_check = @check_for_logreader
		if @retval <> 0 or @@error <> 0
			return (1)
	end
	--coming from sp_cdc_enable_table, when db is published for tran and there is logreader agent already, don't raiserror
	if (@logreader_exists = 1) and (@check_for_logreader = 1)
		return 0
	
	-- Set job specific defaults
	if (@job_type = N'capture')
	begin
		if (@continuous is null)
		begin
			if (serverproperty('EngineEdition') not in (5,12))
			begin
			set @continuous = 1
			end
			else
			begin
				set @continuous = 0
			end
		end
			
		-- Check whether to use 10k as maxtrans fs enabled
		declare @is_use_10k_as_maxtrans_enabled int = 0;
		exec @is_use_10k_as_maxtrans_enabled = sys.sp_is_featureswitch_enabled N'ReplicationUse10kAsMaxTrans';

		if @maxtrans is null
		begin
			if (@is_use_10k_as_maxtrans_enabled = 1)
				set @maxtrans = 10000
			else
				set @maxtrans = 500
		end
			
		if (@maxscans is null)
		begin
			if (serverproperty('EngineEdition') in (5,8,12))
				set @maxscans = 100
			else
				set @maxscans = 10
		end

		if (@pollinginterval is null)
		begin
			if (@continuous = 1)
				set @pollinginterval = 5
			else
				set @pollinginterval = 0	
		end		
	end
	else 
	begin
		if (@retention is null)
			set @retention = 4320
		if (@threshold is null)
			set @threshold = 4999	
	end

	-- Only @retention, @threshold, and @start_job may have non-null values for cleanup job
    if (@job_type = N'cleanup') and 
		( (@pollinginterval is not null) or
		  (@maxtrans is not null) or
		  (@continuous is not null) or
		  (@maxscans is not null))
	begin
		raiserror(22996, 16, -1)
		return(1)
	end	
	
	-- Only @pollinginterval, @maxtrans, @maxscans, @continuous, and @start_job may have non-null values for capture job
    if (@job_type = N'capture') and ((@retention is not null) or (@threshold is not null)) 
	begin
		raiserror(22995, 16, -1)
		return(1)
	end
	
	if (@job_type = N'capture')
		select @retention = 0, @threshold = 0
	else
		select @pollinginterval = 0, @maxtrans = 0, @continuous = 0, @maxscans = 0
	
    -- 86399 seconds maximum on polling interval
    if (@pollinginterval >= (60*60*24)) or (@pollinginterval < 0)
	begin
		raiserror(22990, 16, -1)
		return(1)
	end		

    -- Retention may not be negative or greater than 52594800 if adding cleanup job
    if (@job_type = N'cleanup') and ((@retention <= 0) or (@retention > 52594800))
	begin
		raiserror(22994, 16, -1)
		return(1)
	end
	
    -- Threshold may not be negative if adding cleanup job
    if (@job_type = N'cleanup') and (@threshold <= 0) 
	begin
		raiserror(22850, 16, -1)
		return(1)
	end
	
	-- maxtrans must be greater than 0
	if (@job_type = N'capture') and (@maxtrans <= 0)
	begin
		raiserror(22991, 16, -1)
		return(1)
	end
	
	-- maxscans must be greater than 0
	if (@job_type = N'capture') and (@maxscans <= 0)
	begin
		raiserror(22970, 16, -1)
		return(1)
	end

	-- check for SQL DB edition or box xcopy nonGB instance running without agent (used for test)
	if (serverproperty('EngineEdition') not in (5,12) and
		(serverproperty('IsXCopyInstance') is null or
		 serverproperty('IsNonGolden') is null or
		 exists(select 1 from [sys].[configurations] where name = N'Agent XPs' and value_in_use = 1)))
	begin
		-- Call internal stored procedure to create the job
		exec @retval = sys.sp_cdc_add_job_internal
			@job_type,
			@start_job,
			@maxtrans,
			@maxscans,
			@continuous,
			@pollinginterval,
			@retention,
			@threshold
	end
	else 
	begin
		-- Call internal stored procedure to create the job
		exec @retval = sys.sp_cdc_add_job_internal_db_scoped
			@job_type,
			@start_job,
			@maxtrans,
			@maxscans,
			@continuous,
			@pollinginterval,
			@retention,
			@threshold
	end

    if @retval <> 0 or @@error <> 0
        return (1)

	-- Only on MI for now
	if (serverproperty('EngineEdition') = 8)
	begin
		begin try
			declare @job_parameters nvarchar(250)
			select @job_parameters = N'start_job: ' + isnull(cast(@start_job as nvarchar(10)), 'null') + N'; maxtrans: ' + isnull(cast(@maxtrans as nvarchar(10)), 'null') +
				N'; maxscans: ' + isnull(cast(@maxscans as nvarchar(10)), 'null') + N'; continuous: ' + isnull(cast(@continuous as nvarchar(5)), 'null') +
				N'; pollinginterval: ' + isnull(cast(@pollinginterval as nvarchar(20)), 'null') + N'; retention: ' + isnull(cast(@retention as nvarchar(20)), 'null') +
				N'; threshold: ' + isnull(cast(@threshold as nvarchar(20)), 'null')

			exec sp_cdc_generate_job_event
				@event = N'cdc_job',
				@event_type = N'add_job',
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
create procedure sys.sp_cdc_add_job
(
    @job_type nvarchar(20),
    @start_job bit = 1,
    @maxtrans int = null,
    @maxscans int = null,
    @continuous bit = null,
    @pollinginterval bigint = null,
    @retention bigint = null,
    @threshold bigint = null,
	@check_for_logreader bit = 0
)
as
begin
    set nocount on

    declare @job_name sysname
           ,@retval   int
           ,@job_id   uniqueidentifier
           ,@old_job_id   uniqueidentifier
           ,@job_step_uid  uniqueidentifier
           ,@index    int
           ,@category_name sysname
           ,@command  nvarchar(1000)
           ,@server   sysname
           ,@databasename sysname
           ,@user     sysname
           ,@schedule_name sysname
           ,@database_id int
           ,@valid_job bit
           ,@description nvarchar(100)
           ,@step_name nvarchar(100)
			,@logreader_exists bit

	-- Verify CDC is supported for this SQL Server edition
    IF ([sys].[fn_cdc_is_supported]() = 1)
    BEGIN
        DECLARE @edition sysname
        SELECT @edition = CONVERT(sysname, SERVERPROPERTY('Edition'))
        RAISERROR(22988, 16, -1, @edition)
        RETURN (1)
    END
    
	--
    -- CDC Job security check 
    --
    exec @retval = [sys].[sp_MScdc_job_security_check] 
    if @retval <> 0 or @@error <> 0
        return (1)
        
    -- Verify database is currently enabled for change data capture
    if ([sys].[fn_cdc_is_db_enabled]() != 1)
    begin
		set @databasename = db_name()
		raiserror(22901, 16, -1, @databasename)
        return 1
    end
            
	set @job_type = rtrim(ltrim(lower(@job_type)))

	--
    -- Parameter validation
    --
    
    -- job type must be either 1 or 2
    if ((@job_type is null) or (@job_type not in (N'capture', N'cleanup')))
	begin
		raiserror(22992, 16, -1, @job_type)
		return(1)
	end
	
	--
    -- Insure that transactional replication is not also enabled for the database
    --
    if (@job_type = N'capture')
    begin
		set @logreader_exists = 0
		exec @retval = [sys].[sp_MScdc_tranrepl_check] @logreader_exists = @logreader_exists output, @skip_remote_check = @check_for_logreader
		if @retval <> 0 or @@error <> 0
			return (1)
	end
	--coming from sp_cdc_enable_table, when db is published for tran and there is logreader agent already, don't raiserror
	if (@logreader_exists = 1) and (@check_for_logreader = 1)
		return 0
	
	-- Set job specific defaults
	if (@job_type = N'capture')
	begin
		if (@continuous is null)
		begin
			if (serverproperty('EngineEdition') <> 5)
			begin
			set @continuous = 1
			end
			else
			begin
				set @continuous = 0
			end
		end
			
		if (@maxtrans is null)
			set @maxtrans = 500
			
		if (@maxscans is null)
		begin
			if (serverproperty('EngineEdition') = 5 or serverproperty('EngineEdition') = 8)
				set @maxscans = 100
			else
				set @maxscans = 10
		end

		if (@pollinginterval is null)
		begin
			if (@continuous = 1)
				set @pollinginterval = 5
			else
				set @pollinginterval = 0	
		end		
	end
	else 
	begin
		if (@retention is null)
			set @retention = 4320
		if (@threshold is null)
			set @threshold = 5000	
	end

	-- Only @retention, @threshold, and @start_job may have non-null values for cleanup job
    if (@job_type = N'cleanup') and 
		( (@pollinginterval is not null) or
		  (@maxtrans is not null) or
		  (@continuous is not null) or
		  (@maxscans is not null))
	begin
		raiserror(22996, 16, -1)
		return(1)
	end	
	
	-- Only @pollinginterval, @maxtrans, @maxscans, @continuous, and @start_job may have non-null values for capture job
    if (@job_type = N'capture') and ((@retention is not null) or (@threshold is not null)) 
	begin
		raiserror(22995, 16, -1)
		return(1)
	end
	
	if (@job_type = N'capture')
		select @retention = 0, @threshold = 0
	else
		select @pollinginterval = 0, @maxtrans = 0, @continuous = 0, @maxscans = 0
	
    -- 86399 seconds maximum on polling interval
    if (@pollinginterval >= (60*60*24)) or (@pollinginterval < 0)
	begin
		raiserror(22990, 16, -1)
		return(1)
	end		

    -- Retention may not be negative or greater than 52594800 if adding cleanup job
    if (@job_type = N'cleanup') and ((@retention <= 0) or (@retention > 52594800))
	begin
		raiserror(22994, 16, -1)
		return(1)
	end
	
    -- Threshold may not be negative if adding cleanup job
    if (@job_type = N'cleanup') and (@threshold <= 0) 
	begin
		raiserror(22850, 16, -1)
		return(1)
	end
	
	-- maxtrans must be greater than 0
	if (@job_type = N'capture') and (@maxtrans <= 0)
	begin
		raiserror(22991, 16, -1)
		return(1)
	end
	
	-- maxscans must be greater than 0
	if (@job_type = N'capture') and (@maxscans <= 0)
	begin
		raiserror(22970, 16, -1)
		return(1)
	end
	
	if (serverproperty('EngineEdition') <> 5)
	begin
	-- Call internal stored procedure to create the job
	exec @retval = sys.sp_cdc_add_job_internal
	    @job_type,
		@start_job,
		@maxtrans,
		@maxscans,
		@continuous,
		@pollinginterval,
		@retention,
		@threshold
	end
	else 
	begin
		-- Call internal stored procedure to create the job
		exec @retval = sys.sp_cdc_add_job_internal_db_scoped
			@job_type,
			@start_job,
			@maxtrans,
			@maxscans,
			@continuous,
			@pollinginterval,
			@retention,
			@threshold
	end
	
    if @retval <> 0 or @@error <> 0
        return (1)

	-- Only on MI for now
	if (serverproperty('EngineEdition') = 8)
	begin
		begin try
			declare @job_parameters nvarchar(250)
			select @job_parameters = N'start_job: ' + isnull(cast(@start_job as nvarchar(10)), 'null') + N'; maxtrans: ' + isnull(cast(@maxtrans as nvarchar(10)), 'null') +
				N'; maxscans: ' + isnull(cast(@maxscans as nvarchar(10)), 'null') + N'; continuous: ' + isnull(cast(@continuous as nvarchar(5)), 'null') +
				N'; pollinginterval: ' + isnull(cast(@pollinginterval as nvarchar(20)), 'null') + N'; retention: ' + isnull(cast(@retention as nvarchar(20)), 'null') +
				N'; threshold: ' + isnull(cast(@threshold as nvarchar(20)), 'null')

			exec sp_cdc_generate_job_event
				@event = N'cdc_job',
				@event_type = N'add_job',
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

