SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/

create procedure [sys].[sp_MScdc_capture_job]
as
begin
	set nocount on
	declare @retcode int
           ,@pollinginterval bigint
           ,@continuous bit
           ,@maxtrans int
           ,@maxscans int
           ,@db_name sysname
		   ,@is_from_job bit

	set @db_name = db_name()

	set @is_from_job = 1

    --
    -- security check - should be dbo or sysadmin
    --
    exec @retcode = sp_MSreplcheck_publish
    if @@ERROR != 0 or @retcode != 0
	begin
		raiserror(22864, 16, -1, @db_name)
		return 1
	end	

    --
    -- security check
    -- Has to be executed from cdc enabled db
    --
	if not sys.fn_cdc_is_enabled_for_current_db () = 0
	begin
		RAISERROR(22901, 16, -1, @db_name)
		return (1)
	end

	--
	-- Insure that transactional replication is not also trying to scan the log
	--
	exec @retcode = [sys].[sp_MScdc_tranrepl_check] 
	if @retcode <> 0 or @@error <> 0
	begin
		raiserror(22864, 16, -1, @db_name)
		return 1
	end	
	
	if (serverproperty('EngineEdition') not in (5,12))
	begin
	-- Get job parameters for msdb.cdc_jobs
	exec @retcode = sys.sp_cdc_get_job_parameters
		@pollinginterval output
		,@continuous  output
		,@maxscans output
		,@maxtrans output
	end
	else
	begin
		-- Get job parameters for cdc.cdc_jobs
		exec @retcode = sys.sp_cdc_get_job_parameters_db_scoped
			@pollinginterval output
			,@continuous  output
			,@maxscans output
			,@maxtrans output
	end

	if (@@error <> 0) or (@retcode <> 0)
	begin
		raiserror(22864, 16, -1, @db_name)
		return 1
	end
	
	if @continuous is null 
		set @continuous = 1

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
		
	if @maxscans is null
	begin
		-- Default to @maxscans = 100 for SQL DB, SQL MI and TridentNative
		if (serverproperty('EngineEdition') in (5,8,12))
			set @maxscans = 100
		else
			set @maxscans = 10
	end

    if @pollinginterval is null
		if @continuous = 1
			set @pollinginterval = 5
		else
			set @pollinginterval = 0

	if (serverproperty('EngineEdition') in (5,12))
	begin
		set @is_from_job = 0
	end

    exec @retcode = sp_cdc_scan
		 @pollinginterval = @pollinginterval
		,@continuous = @continuous
		,@maxtrans = @maxtrans
        ,@maxscans = @maxscans
        ,@is_from_job = @is_from_job
	
	if @retcode <> 0 or @@error <> 0
	begin
		raiserror(22864, 16, -1, @db_name)
		return 1
	end
		
	return 0
END


/*====  SQL Server 2022 version  ====*/

create procedure [sys].[sp_MScdc_capture_job]
as
begin
	set nocount on
	declare @retcode int
           ,@pollinginterval bigint
           ,@continuous bit
           ,@maxtrans int
           ,@maxscans int
           ,@db_name sysname
		   ,@is_from_job bit

	set @db_name = db_name()

	set @is_from_job = 1

    --
    -- security check - should be dbo or sysadmin
    --
    exec @retcode = sp_MSreplcheck_publish
    if @@ERROR != 0 or @retcode != 0
	begin
		raiserror(22864, 16, -1, @db_name)
		return 1
	end	

    --
    -- security check
    -- Has to be executed from cdc enabled db
    --
	if not sys.fn_cdc_is_enabled_for_current_db () = 0
	begin
		RAISERROR(22901, 16, -1, @db_name)
		return (1)
	end

	--
	-- Insure that transactional replication is not also trying to scan the log
	--
	exec @retcode = [sys].[sp_MScdc_tranrepl_check] 
	if @retcode <> 0 or @@error <> 0
	begin
		raiserror(22864, 16, -1, @db_name)
		return 1
	end	
	
	if (serverproperty('EngineEdition') <> 5)
	begin
	-- Get job parameters for msdb.cdc_jobs
	exec @retcode = sys.sp_cdc_get_job_parameters
		@pollinginterval output
		,@continuous  output
		,@maxscans output
		,@maxtrans output
	end
	else
	begin
		-- Get job parameters for cdc.cdc_jobs
		exec @retcode = sys.sp_cdc_get_job_parameters_db_scoped
			@pollinginterval output
			,@continuous  output
			,@maxscans output
			,@maxtrans output
	end

	if (@@error <> 0) or (@retcode <> 0)
	begin
		raiserror(22864, 16, -1, @db_name)
		return 1
	end
	
	if @continuous is null 
		set @continuous = 1
			
	if @maxtrans is null
		set @maxtrans = 500
		
	if @maxscans is null
	begin
		-- Default to @maxscans = 100 for SQL DB and SQL MI
		if (serverproperty('EngineEdition') = 5 or serverproperty('EngineEdition') = 8)
			set @maxscans = 100
		else
			set @maxscans = 10
	end

    if @pollinginterval is null
		if @continuous = 1
			set @pollinginterval = 5
		else
			set @pollinginterval = 0

	if (serverproperty('EngineEdition') = 5)
	begin
		set @is_from_job = 0
	end

    exec @retcode = sp_cdc_scan
		 @pollinginterval = @pollinginterval
		,@continuous = @continuous
		,@maxtrans = @maxtrans
        ,@maxscans = @maxscans
        ,@is_from_job = @is_from_job
	
	if @retcode <> 0 or @@error <> 0
	begin
		raiserror(22864, 16, -1, @db_name)
		return 1
	end
		
	return 0
END

