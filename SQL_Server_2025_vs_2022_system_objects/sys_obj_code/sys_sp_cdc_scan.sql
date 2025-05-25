use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/

create procedure [sys].[sp_cdc_scan]
(
	@maxtrans int = 10000 -- maximum # of committed transactions to scan for
	,@maxscans int = 100 -- maximum # of scans to perform
    ,@continuous tinyint = 0
    ,@pollinginterval bigint = 0
    ,@is_from_job int = 0
)
as
begin
	set nocount on
	declare @retcode int
		,@xact_seqno varbinary(16)
		,@xact_id varbinary(16)
		,@hours int
		,@minutes int
		,@seconds int
		,@waittime nvarchar(100)
		,@scancnt int
		,@job_id uniqueidentifier
		,@run_date INT
		,@run_time INT
		,@run_duration INT
		,@run_duration_in_sec INT
		,@run_duration_in_hh INT
		,@run_duration_in_mm INT
		,@run_duration_in_ss INT
		,@message nvarchar(1024)
		,@command_count int
		,@tran_count int
		,@start_lsn nvarchar(22)
		,@end_lsn nvarchar(22)
		,@start_time datetime
		,@current_time datetime
		,@db_name sysname
		,@activetrancount int

    --
    -- security check - should be dbo or sysadmin
    --
    exec @retcode = sp_MSreplcheck_publish
    if @@ERROR != 0 or @retcode != 0
        return 1

    --
    -- security check
    -- Has to be executed from cdc enabled db
    --
	if not sys.fn_cdc_is_enabled_for_current_db () = 0
	begin
		set @db_name = db_name()
		RAISERROR(22901, 16, -1, @db_name)
		return (1)
	end

	--
	-- Insure that transactional replication is not also trying to scan the log
	--
	exec @retcode = [sys].[sp_MScdc_tranrepl_check] 
	if @retcode <> 0 or @@error <> 0
		return (1)
	
	declare @sp_replcmds table
	(
		[article id] int
		,[partial_command] bit
		,[command] varbinary(2000)
		,[xactid] varbinary(16)
		,[xact_seqno] varbinary(16)
		,[publication_id] int
		,[command_id] int
		,[command_type] int
		,[originator_srvname] sysname
		,[originator_db] sysname
		,[pkHash] int
		,[originator_publication_id] int
		,[originator_db_version] int
		,[originator_lsn] varbinary(16)
	)
	
	-- Parameter validation
	if (@continuous is null) or (@continuous not in (0, 1))
	begin
		raiserror(22998, 16, -1)
		return (1)
	end
	
	if (@continuous = 0)
	begin 
		if  ((@pollinginterval is not null) and (@pollinginterval <> 0))
		begin
			raiserror(22999, 16, -1)
			return (1)
		end
	end
	else	
	begin 
		if  (@pollinginterval is null) or (@pollinginterval >= (60*60*24)) or (@pollinginterval < 0)
		begin
			raiserror(22990, 16, -1)
			return (1)
		end
	end

	if ((@maxtrans is null) or (@maxtrans <= 0))
	begin
		raiserror(22991, 16, -1)
		return (1)
	end
	
	if ((@maxscans is null) or (@maxscans <= 0))
	begin
		raiserror(22970, 16, -1)
		return (1)
	end

    if (@continuous = 1)
    begin
                   
		select @seconds = @pollinginterval % 60
		select @minutes = ((@pollinginterval - @seconds) / 60) % 60
		select @hours = ((@pollinginterval - (@minutes * 60) - @seconds) / 60) / 60

		select @waittime = convert(nvarchar(10), @hours) + N':' +
						   convert(nvarchar(2), @minutes) + N':' +
                           convert(nvarchar(2), @seconds)
	end

	select @run_duration = 0
			,@start_time = getdate()

	--query run_date, run_time from step 1 instead of computing ourselves, better chance to align with mpu format
	if (@is_from_job = 1 and serverproperty('EngineEdition') not in (5,12)) 
    begin
		-- Call function to get jobid and access msdb 
        select @job_id = job_id, @run_date = run_date, @run_time = run_time 
		from [sys].[fn_cdc_get_jobid]()
    end
    else
    begin
        select @job_id = null
    end
    
	set @activetrancount = @@TRANCOUNT    

	set @xact_seqno = NULL
	select @xact_seqno = MAX(start_lsn) from cdc.lsn_time_mapping where tran_id != 0x0
	if(@xact_seqno is not NULL)
	begin
		-- Made change as per VSTS 14118995. Need to check the return code to avoid silent failure
		EXEC @retcode = sp_repldone @xactid = 0x, @xact_segno = @xact_seqno, @numtrans = 0,    @time = 0
	    	if @@ERROR <> 0 or @retcode <> 0
	    	begin 
    			set @retcode = 1
	    		goto Failure
    		end	
    	end

    while 0 <> 1
    begin
		set @scancnt = 0
    
		while (@scancnt < @maxscans)
		begin

			BEGIN TRAN
			SAVE TRAN tr_sp_cdc_scan

			-- Instead of having this 2 places, moved it to top so we always delete from table variable before populating it
			delete from @sp_replcmds
			
			insert @sp_replcmds exec sp_replcmds @maxtrans
	    		if @@ERROR <> 0 
    			begin 
    				set @retcode = 1
    				goto Failure
    			end

			-- Made change as per VSTS 14118995. sp_repldone doesn't follow transaction semantics so moving it out of transaction
			COMMIT TRAN    		
				
    			select @xact_id = null, @xact_seqno = null
    			select @xact_id = xactid, @xact_seqno = xact_seqno  from @sp_replcmds
    			if (@xact_id is not null and @xact_seqno is not null)
    			begin 
    				EXEC @retcode = sp_repldone @xactid = @xact_id, @xact_segno = @xact_seqno, @numtrans = 0,    @time = 0
    				if @@ERROR <> 0 or @retcode <> 0
    				begin 
    					set @retcode = 1
    					goto Failure
    				end

			-- If Azure then we don't have access to msdb
			if((@job_id is not null) and (serverproperty('EngineEdition') not in (5,12)))
				begin

					select top 1 @command_count = command_count, @tran_count = tran_count
							,@start_lsn = start_lsn, @end_lsn = end_lsn
					from sys.dm_cdc_log_scan_sessions order by session_id desc

		    			select @message = isnull(formatmessage(22803, @start_lsn, @end_lsn, @tran_count, @command_count), N'Message 22803')
								,@current_time = getdate()

					--sysjobhistory.run_duration  int  Elapsed time in the execution of the job or step in HHMMSS format.
    		 			select @run_duration_in_sec = datediff(ss, @start_time, @current_time)
    					select @run_duration_in_hh = (@run_duration_in_sec/60)/60
    					select @run_duration_in_mm = @run_duration_in_sec/60 - @run_duration_in_hh*60 
    					select @run_duration_in_ss = @run_duration_in_sec - (@run_duration_in_hh*60 +  @run_duration_in_mm) * 60
    					select @run_duration = @run_duration_in_hh * 10000 + @run_duration_in_mm * 100 + @run_duration_in_sec
    
					exec [sys].[sp_cdc_sqlagent_log_jobhistory]
    						  @job_id               = @job_id,
    						  @step_id              = 2,
    						  @sql_message_id       = 22803,
    						  @sql_severity         = 10,
    						  @message              = @message,
	    					  @run_status           = 4,
    						  @run_date             = @run_date,
    						  @run_time             = @run_time,
    						  @run_duration         = @run_duration,
    						  @retries_attempted    = 0,
    						  @server               = @@servername
    		
                		end
            		end
			
    		-- Check here to determine whether log has been drained
    		-- If log drained, exit loop
    		if (@xact_id is null) or (@xact_seqno is null)
    		begin
    			goto Sleep
    		end	
    		
    		set @scancnt = @scancnt + 1
    	end	

Sleep:
		if @continuous <> 0
			waitfor delay @waittime
		else
			return @retcode

    end

Failure:
		if (@@TRANCOUNT > @activetrancount)
		begin
			if XACT_STATE() = 1
			begin
				rollback tran tr_sp_cdc_scan
				commit tran
			end
			else if XACT_STATE() = -1
			begin
				rollback tran
			end
		end
		
		-- If Azure then we don't have access to msdb
		if((@job_id is not null) and (serverproperty('EngineEdition') not in (5,12)))
		begin
			declare @session_id int
						,@error_id int
						,@sev int
						,@state int
						,@entry_time datetime
						
			select top 1 @session_id = session_id
			from sys.dm_cdc_log_scan_sessions order by session_id desc
									
			declare #herror cursor local fast_forward
			for
				select error_number, error_severity, error_state, error_message, entry_time
				from sys.dm_cdc_errors where session_id = @session_id
				order by entry_time ASC

			open #herror
			fetch #herror into @error_id, @sev, @state, @message, @entry_time

			while (@@fetch_status <> -1)
			begin

				if(@sev > 10)
				begin
					select @message = N'Entry Time ' + convert(nvarchar, @entry_time, 121 ) +  N', Msg ' + cast (@error_id as nvarchar ) + N', Level ' + cast (@sev as nvarchar) + N', State ' + cast (@state as nvarchar) + N', ' + @message + N' ' + formatmessage(22805)

					exec [sys].[sp_cdc_sqlagent_log_jobhistory]
					  @job_id               = @job_id,
					  @step_id              = 2,
					  @sql_message_id       = @error_id,
					  @sql_severity         = @sev,
					  @message              = @message,
					  @run_status           = 0,
					  @run_date             = @run_date,
					  @run_time             = @run_time,
					  @run_duration         = @run_duration,
					  @retries_attempted    = 0,
					  @server               = @@servername
				end
				fetch #herror into @error_id, @sev, @state, @message, @entry_time

			end
				
				
			close #herror
			deallocate #herror
			
		end	
	return @retcode
END


/*====  SQL Server 2022 version  ====*/

create procedure [sys].[sp_cdc_scan]
(
	@maxtrans int = 500 -- maximum # of committed transactions to scan for
	,@maxscans int = 10 -- maximum # of scans to perform
    ,@continuous tinyint = 0
    ,@pollinginterval bigint = 0
    ,@is_from_job int = 0
)
as
begin
	set nocount on
	declare @retcode int
		,@xact_seqno varbinary(16)
		,@xact_id varbinary(16)
		,@hours int
		,@minutes int
		,@seconds int
		,@waittime nvarchar(100)
		,@scancnt int
		,@job_id uniqueidentifier
		,@run_date INT
		,@run_time INT
		,@run_duration INT
		,@run_duration_in_sec INT
		,@run_duration_in_hh INT
		,@run_duration_in_mm INT
		,@run_duration_in_ss INT
		,@message nvarchar(1024)
		,@command_count int
		,@tran_count int
		,@start_lsn nvarchar(22)
		,@end_lsn nvarchar(22)
		,@start_time datetime
		,@current_time datetime
		,@db_name sysname
		,@activetrancount int

    --
    -- security check - should be dbo or sysadmin
    --
    exec @retcode = sp_MSreplcheck_publish
    if @@ERROR != 0 or @retcode != 0
        return 1

    --
    -- security check
    -- Has to be executed from cdc enabled db
    --
	if not sys.fn_cdc_is_enabled_for_current_db () = 0
	begin
		set @db_name = db_name()
		RAISERROR(22901, 16, -1, @db_name)
		return (1)
	end

	--
	-- Insure that transactional replication is not also trying to scan the log
	--
	exec @retcode = [sys].[sp_MScdc_tranrepl_check] 
	if @retcode <> 0 or @@error <> 0
		return (1)
	
	declare @sp_replcmds table
	(
		[article id] int
		,[partial_command] bit
		,[command] varbinary(2000)
		,[xactid] varbinary(16)
		,[xact_seqno] varbinary(16)
		,[publication_id] int
		,[command_id] int
		,[command_type] int
		,[originator_srvname] sysname
		,[originator_db] sysname
		,[pkHash] int
		,[originator_publication_id] int
		,[originator_db_version] int
		,[originator_lsn] varbinary(16)
	)
	
	-- Parameter validation
	if (@continuous is null) or (@continuous not in (0, 1))
	begin
		raiserror(22998, 16, -1)
		return (1)
	end
	
	if (@continuous = 0)
	begin 
		if  ((@pollinginterval is not null) and (@pollinginterval <> 0))
		begin
			raiserror(22999, 16, -1)
			return (1)
		end
	end
	else	
	begin 
		if  (@pollinginterval is null) or (@pollinginterval >= (60*60*24)) or (@pollinginterval < 0)
		begin
			raiserror(22990, 16, -1)
			return (1)
		end
	end

	if ((@maxtrans is null) or (@maxtrans <= 0))
	begin
		raiserror(22991, 16, -1)
		return (1)
	end
	
	if ((@maxscans is null) or (@maxscans <= 0))
	begin
		raiserror(22970, 16, -1)
		return (1)
	end

    if (@continuous = 1)
    begin
                   
		select @seconds = @pollinginterval % 60
		select @minutes = ((@pollinginterval - @seconds) / 60) % 60
		select @hours = ((@pollinginterval - (@minutes * 60) - @seconds) / 60) / 60

		select @waittime = convert(nvarchar(10), @hours) + N':' +
						   convert(nvarchar(2), @minutes) + N':' +
                           convert(nvarchar(2), @seconds)
	end

	select @run_duration = 0
			,@start_time = getdate()

	--query run_date, run_time from step 1 instead of computing ourselves, better chance to align with mpu format
	if (@is_from_job = 1 and serverproperty('EngineEdition') <> 5) 
    begin
		-- Call function to get jobid and access msdb 
        select @job_id = job_id, @run_date = run_date, @run_time = run_time 
		from [sys].[fn_cdc_get_jobid]()
    end
    else
    begin
        select @job_id = null
    end
    
	set @activetrancount = @@TRANCOUNT    

	set @xact_seqno = NULL
	select @xact_seqno = MAX(start_lsn) from cdc.lsn_time_mapping where tran_id != 0x0
	if(@xact_seqno is not NULL)
	begin
		-- Made change as per VSTS 14118995. Need to check the return code to avoid silent failure
		EXEC @retcode = sp_repldone @xactid = 0x, @xact_segno = @xact_seqno, @numtrans = 0,    @time = 0
	    	if @@ERROR <> 0 or @retcode <> 0
	    	begin 
    			set @retcode = 1
	    		goto Failure
    		end	
    	end

    while 0 <> 1
    begin
		set @scancnt = 0
    
		while (@scancnt < @maxscans)
		begin

			BEGIN TRAN
			SAVE TRAN tr_sp_cdc_scan

			-- Instead of having this 2 places, moved it to top so we always delete from table variable before populating it
			delete from @sp_replcmds
			
			insert @sp_replcmds exec sp_replcmds @maxtrans
	    		if @@ERROR <> 0 
    			begin 
    				set @retcode = 1
    				goto Failure
    			end

			-- Made change as per VSTS 14118995. sp_repldone doesn't follow transaction semantics so moving it out of transaction
			COMMIT TRAN    		
				
    			select @xact_id = null, @xact_seqno = null
    			select @xact_id = xactid, @xact_seqno = xact_seqno  from @sp_replcmds
    			if (@xact_id is not null and @xact_seqno is not null)
    			begin 
    				EXEC @retcode = sp_repldone @xactid = @xact_id, @xact_segno = @xact_seqno, @numtrans = 0,    @time = 0
    				if @@ERROR <> 0 or @retcode <> 0
    				begin 
    					set @retcode = 1
    					goto Failure
    				end

			-- If Azure then we don't have access to msdb
			if((@job_id is not null) and (serverproperty('EngineEdition') <> 5))
				begin

					select top 1 @command_count = command_count, @tran_count = tran_count
							,@start_lsn = start_lsn, @end_lsn = end_lsn
					from sys.dm_cdc_log_scan_sessions order by session_id desc

		    			select @message = isnull(formatmessage(22803, @start_lsn, @end_lsn, @tran_count, @command_count), N'Message 22803')
								,@current_time = getdate()

					--sysjobhistory.run_duration  int  Elapsed time in the execution of the job or step in HHMMSS format.
    		 			select @run_duration_in_sec = datediff(ss, @start_time, @current_time)
    					select @run_duration_in_hh = (@run_duration_in_sec/60)/60
    					select @run_duration_in_mm = @run_duration_in_sec/60 - @run_duration_in_hh*60 
    					select @run_duration_in_ss = @run_duration_in_sec - (@run_duration_in_hh*60 +  @run_duration_in_mm) * 60
    					select @run_duration = @run_duration_in_hh * 10000 + @run_duration_in_mm * 100 + @run_duration_in_sec
    
					exec [sys].[sp_cdc_sqlagent_log_jobhistory]
    						  @job_id               = @job_id,
    						  @step_id              = 2,
    						  @sql_message_id       = 22803,
    						  @sql_severity         = 10,
    						  @message              = @message,
	    					  @run_status           = 4,
    						  @run_date             = @run_date,
    						  @run_time             = @run_time,
    						  @run_duration         = @run_duration,
    						  @retries_attempted    = 0,
    						  @server               = @@servername
    		
                		end
            		end
			
    		-- Check here to determine whether log has been drained
    		-- If log drained, exit loop
    		if (@xact_id is null) or (@xact_seqno is null)
    		begin
    			goto Sleep
    		end	
    		
    		set @scancnt = @scancnt + 1
    	end	

Sleep:
		if @continuous <> 0
			waitfor delay @waittime
		else
			return @retcode

    end

Failure:
		if (@@TRANCOUNT > @activetrancount)
		begin
			if XACT_STATE() = 1
			begin
				rollback tran tr_sp_cdc_scan
				commit tran
			end
			else if XACT_STATE() = -1
			begin
				rollback tran
			end
		end
		
		-- If Azure then we don't have access to msdb
		if((@job_id is not null) and (serverproperty('EngineEdition') <> 5))
		begin
			declare @session_id int
						,@error_id int
						,@sev int
						,@state int
						,@entry_time datetime
						
			select top 1 @session_id = session_id
			from sys.dm_cdc_log_scan_sessions order by session_id desc
									
			declare #herror cursor local fast_forward
			for
				select error_number, error_severity, error_state, error_message, entry_time
				from sys.dm_cdc_errors where session_id = @session_id
				order by entry_time ASC

			open #herror
			fetch #herror into @error_id, @sev, @state, @message, @entry_time

			while (@@fetch_status <> -1)
			begin

				if(@sev > 10)
				begin
					select @message = N'Entry Time ' + convert(nvarchar, @entry_time, 121 ) +  N', Msg ' + cast (@error_id as nvarchar ) + N', Level ' + cast (@sev as nvarchar) + N', State ' + cast (@state as nvarchar) + N', ' + @message + N' ' + formatmessage(22805)

					exec [sys].[sp_cdc_sqlagent_log_jobhistory]
					  @job_id               = @job_id,
					  @step_id              = 2,
					  @sql_message_id       = @error_id,
					  @sql_severity         = @sev,
					  @message              = @message,
					  @run_status           = 0,
					  @run_date             = @run_date,
					  @run_time             = @run_time,
					  @run_duration         = @run_duration,
					  @retries_attempted    = 0,
					  @server               = @@servername
				end
				fetch #herror into @error_id, @sev, @state, @message, @entry_time

			end
				
				
			close #herror
			deallocate #herror
			
		end	
	return @retcode
END

