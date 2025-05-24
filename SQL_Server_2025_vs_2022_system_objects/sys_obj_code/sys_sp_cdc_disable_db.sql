SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/
create procedure [sys].[sp_cdc_disable_db]
as
begin
	declare @retcode int
		,@db_name sysname
		,@user_privileges_needed nvarchar(1000)
		,@status int
		,@traceMessage nvarchar(4000)
		,@replNoopLandmarkAfterCDCDisableLSN binary(10) = null
		,@applock_result int
        ,@resource nvarchar(255)
        ,@action nvarchar(1000)
		,@db_id int
		,@trancount int
		,@raised_error int
		,@raised_message nvarchar(4000)
		,@is_cdc_fix_disable_db_contention_FS_enabled int

	set @db_id = db_id()
	set @resource = N'__$cdc__db_' + convert(nvarchar(10),@db_id)
	set @raised_error = 0

	-- Verify caller is authorized to disable change data capture for the database
	if ([sys].[fn_cdc_is_user_authorized]() = 0)
	begin
		select @user_privileges_needed = [sys].[fn_cdc_get_user_privileges]()
		raiserror(22902, 16, -1, @user_privileges_needed)
		return 1
	end

	-- Verify database is currently enabled for change data capture
	if ([sys].[fn_cdc_is_db_enabled]() != 1)
	begin
		set @db_name = db_name()
		raiserror(22901, 10, -1, @db_name)
		return 0
	end

	BEGIN TRY
		exec sys.sp_cdc_fire_trace_event 0, -1, N'sp_cdc_disable_db', N'entering'

		-- if Azure sql db or test xcopy we should stop(resume on fail) the capture job and cleanup job to allow disable to proceed
		if([sys].[fn_cdc_allow_safe_disable_db_featureswitch_is_enabled]() = 1 and
		(cast(serverproperty('EditionId') as int) = 0x63CCFCE6 and serverproperty('EngineEdition') in (3,5,12)
			or (serverproperty('IsXCopyInstance') is not null and serverproperty('IsNonGolden') is not null and
				[sys].[fn_cdc_allow_job_enqueue_pause_featureswitch_is_enabled]() = 1)))
		begin
			set @trancount = @@trancount

			begin tran 
			save tran sp_cdc_disable_db

			-- If FS - ChangeDataCaptureFixDisableDbContention is ON, we dont allow mutiple disable operations to run in parallel
			-- by using the applock
			exec @is_cdc_fix_disable_db_contention_FS_enabled = sys.sp_is_featureswitch_enabled N'ChangeDataCaptureFixDisableDbContention';
			if(@is_cdc_fix_disable_db_contention_FS_enabled = 1)
			begin
				exec sys.sp_cdc_fire_trace_event 0, -1, N'sp_cdc_disable_db', N'Attempting to get an applock'

				--  Get exclusive database lock
				exec @applock_result = sys.sp_getapplock @Resource = @resource, @LockMode = N'Exclusive',
					@LockOwner = 'Transaction', @DbPrincipal = 'db_owner'
		
				If @applock_result < 0
				begin
					-- Lock request failed.
					set @action = N'sys.sp_getapplock @Resource = ' + @resource + N'@LockMode = N''Exclusive'', @LockOwner = ''Transaction'', @DbPrincipal = ''db_owner'' ' 
					raiserror(22840, 16, -1, @action, @applock_result)
				end
			end

			exec sys.sp_cdc_fire_trace_event 0, -1, N'sp_cdc_disable_db', N'executing extended proc'
			set @action = N'sys.sp_cdc_disable_db_safe'
			exec @retcode = sys.sp_cdc_disable_db_safe

			if (@retcode <> 0)
			begin
				if @@trancount > @trancount
				begin
					-- If CDC opened the transaction or it is not possible 
					-- to rollback to the savepoint, rollback the transaction
					if ( @trancount = 0 ) OR ( XACT_STATE() <> 1 )
					begin
						rollback tran
						exec sys.sp_cdc_fire_trace_event 0, -1, N'sp_cdc_disable_db', N'The call to sp_cdc_disable_db_safe has failed. Rolled back the transaction'
					end
					-- Otherwise rollback to the savepoint
					else
					begin
						rollback tran sp_cdc_disable_db
						commit tran
						exec sys.sp_cdc_fire_trace_event 0, -1, N'sp_cdc_disable_db', N'The call to sp_cdc_disable_db_safe has failed. Rolled back the transaction: sp_cdc_disable_db'
					end
				end
			end
			else
			begin
				commit tran
			end
		end
		else
		begin
			set @action = N'sys.sp_cdc_disable_db_internal'
			exec sys.sp_cdc_fire_trace_event 0, -1, N'sp_cdc_disable_db', N'executing internal proc'
			exec @retcode = sys.sp_cdc_disable_db_internal
		end

		set @status = @@error
		if @status = 0 set @status = @retcode
		exec sys.sp_cdc_fire_trace_event 0, @status, N'sp_cdc_disable_db', N'complete'

		if (@status <> 0)
		begin
			return 1
		end

		--Marking the last CDC replication ending log point in the t-log with a no-operation log which is indentified with the LSN returned by the sp_replincrementlsn_internal function. Emitted in trace.
		set @action = N'sys.sp_replincrementlsn_internal'
		exec @retcode = sys.sp_replincrementlsn_internal @replNoopLandmarkAfterCDCDisableLSN OUTPUT
		set @status = @@error
		if (@retcode != 0) or (@@error != 0) or (@replNoopLandmarkAfterCDCDisableLSN IS NULL)
		begin
			set @raised_error = @status
			set @traceMessage = N'sp_replincrementlsn_internal failed in generating a No-op LSN for landmarking CDC disable, or the landmark LSN is returned NULL'
			exec sys.sp_cdc_fire_trace_event 0, @status, N'sp_cdc_disable_db', @traceMessage;
		end
		else 
		begin 
			set @traceMessage = N'CDC Disable completed, next landmark lsn: ' + CONVERT( varchar, @replNoopLandmarkAfterCDCDisableLSN , 1);
			exec sys.sp_cdc_fire_trace_event 0, @status, N'sp_cdc_disable_db', @traceMessage;
		end
	END TRY
	BEGIN CATCH
		-- Save the error number and associated message raised in the TRY block
		select @raised_error = ERROR_NUMBER()
		select @raised_message = ERROR_MESSAGE()
		exec sys.sp_cdc_fire_trace_event 0, @raised_error, N'sp_cdc_disable_db', @raised_message

		if @@trancount > @trancount
		begin
			-- If CDC opened the transaction or it is not possible 
			-- to rollback to the savepoint, rollback the transaction
			if ( @trancount = 0 ) OR ( XACT_STATE() <> 1 )
			begin
				rollback tran
				exec sys.sp_cdc_fire_trace_event 0, -1, N'sp_cdc_disable_db', N'Caught an exception. Rolled back the transaction'
			end
			-- Otherwise rollback to the savepoint
			else
			begin
				rollback tran sp_cdc_disable_db
				commit tran
				exec sys.sp_cdc_fire_trace_event 0, -1, N'sp_cdc_disable_db', N'Caught an exception. Rolled back the transaction: sp_cdc_disable_db'
			end
		end
		
		exec sys.sp_cdc_fire_trace_event 0, @raised_error, N'sp_cdc_disable_db', N'caught an exception'
	END CATCH

	if @raised_error = 0 and @retcode = 0
	begin
		return 0
	end
	begin
		raiserror(22896, 16, -1, @action, @raised_error, @raised_message)  
		return 1
	end
end


/*====  SQL Server 2022 version  ====*/
create procedure [sys].[sp_cdc_disable_db]
as
begin
	declare @retcode int
		,@db_name sysname
		,@user_privileges_needed nvarchar(1000)
  
    -- Verify caller is authorized to disable change data capture for the database 
    if ([sys].[fn_cdc_is_user_authorized]() = 0)
    begin
		select @user_privileges_needed = [sys].[fn_cdc_get_user_privileges]()
   		raiserror(22902, 16, -1, @user_privileges_needed)
        return 1
    end
    
    -- Verify database is currently enabled for change data capture
    if ([sys].[fn_cdc_is_db_enabled]() != 1)
    begin
		set @db_name = db_name()
		raiserror(22901, 10, -1, @db_name)
        return 0
    end
    
    exec @retcode = sys.sp_cdc_disable_db_internal
    
    if (@@error <> 0) or (@retcode <> 0)
    begin
		return 1
	end
	
	return 0
end

