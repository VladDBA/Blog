use [master]
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2022 CU20 version  ====*/

create procedure sys.sp_dbmmonitorupdate
(
	@database_name		sysname = null	-- if null update all mirrored databases
)
as
begin
	set nocount on
	if (is_srvrolemember(N'sysadmin') <> 1 )
    begin
		raiserror(21089, 16, 1)
		return 1
	end
	if ( db_name() != N'msdb' )
	begin
		raiserror(32045, 16, 1, N'sys.sp_dbmmonitorupdate')
		return 1
	end

	declare		@retcode	int

	if object_id ( N'msdb.dbo.dbm_monitor_data', N'U' ) is null
	begin
		create table msdb.dbo.dbm_monitor_data (		-- go through the code to see if there SHOULD be nulls.
			database_id				smallint,
			role					bit null,
			status					tinyint null,
			witness_status			tinyint null,
			log_flush_rate			bigint null,
			send_queue_size			bigint null,
			send_rate				bigint null,
			redo_queue_size			bigint null,
			redo_rate				bigint null,
			transaction_delay		bigint null,
			transactions_per_sec	bigint null,
			time					datetime,
			end_of_log_lsn			numeric(25,0),
			failover_lsn			numeric(25,0),
			local_time				datetime
		)

		exec @retcode = msdb.dbo.sp_MS_marksystemobject dbm_monitor_data
		if ( @@error != 0 OR @retcode != 0 )
		begin
			raiserror( 32038, 16, 1 )
			return 1
		end

		create clustered index dbmmonitor1
            on msdb.dbo.dbm_monitor_data (database_id asc, time asc )
	end

	-- TO DO: create some keys depending on enterences.
	-- TO DO: make sure that the rows are unique
	if object_id ( N'msdb.dbo.dbm_monitor_alerts', N'U' ) is null
	begin
		create table msdb.dbo.dbm_monitor_alerts (
			database_id				smallint,
			retention_period		int null,	--this defaults to 7 days.  checked during the table update
			time_behind				int null,
			enable_time_behind		bit null,
			send_queue				int null,
			enable_send_queue		bit null,
			redo_queue				int null,
			enable_redo_queue		bit null,
			average_delay			int null,
			enable_average_delay	bit null
		)

		exec @retcode = msdb.dbo.sp_MS_marksystemobject dbm_monitor_alerts
		if ( @@error != 0 OR @retcode != 0 )
		begin
			raiserror( 32038, 16, 2 )
			return 1
		end
	end

	if ( select name from sys.database_principals where name = N'dbm_monitor') is null
	begin
		create role dbm_monitor
		grant select on object::msdb.dbo.dbm_monitor_data to dbm_monitor
	end

	if @database_name is not null
	begin
		--
		-- Check if the database specified exists
		--
		if not exists (select * from master.sys.databases where name = @database_name)
		begin
			raiserror(15010, 16, 1, @database_name)
			return 1
		end
		--
		-- Check to see if it is mirrored
		--
		if (select mirroring_guid from master.sys.database_mirroring where database_id = db_id(@database_name)) is null
		begin
			raiserror(32039, 16, 1, @database_name)
			return 1
		end

	declare
		@database_id			smallint,
		@role					bit,
		@status					tinyint,
		@witness_status			tinyint,
		@log_flush_rate			bigint ,
		@send_queue_size		bigint ,
		@send_rate				bigint ,
		@redo_queue_size		bigint ,
		@redo_rate				bigint ,
		@transaction_delay		bigint ,
		@transactions_per_sec	bigint ,
		@time					datetime ,
		@end_of_log_lsn			numeric(25,0),
		@failover_lsn			numeric(25,0),
		@local_time				datetime

	declare
		@retention_period		int,
		@oldest_date			datetime

		set @database_id = DB_ID( @database_name )

-- To select the correct perf counter, we need the instance name.
		declare
			@perf_instance1		nvarchar(256),
			@perf_instance2		nvarchar(256),
			@instance			nvarchar(128)

		select @instance = convert( nvarchar,  serverproperty(N'instancename'))
		if @instance is null
		begin
			set @instance = N'SQLServer'
		end
		else
		begin
			set @instance = N'MSSQL$' + @instance
		end

		set @perf_instance1 = left(@instance, len(@instance)) + N':Database Mirroring'
		set @perf_instance2 = left(@instance, len(@instance)) + N':Databases'

--
-- Insert a single row in the table for each database
--
-- 1. Pull out the perf counters
-- 2. Pull out the information from sys.database_mirroring
-- 3. Get the end of log lsn

		declare @perfcounters table(
			counter_name		nchar(128),
			cntr_value			bigint
		)

		insert into @perfcounters select counter_name, cntr_value from sys.dm_os_performance_counters where
			(object_name = @perf_instance1 or object_name = @perf_instance2 ) and
			instance_name = @database_name and
			counter_name IN (N'Log Send Queue KB', N'Log Bytes Sent/sec', N'Redo Queue KB', N'Redo Bytes/sec', N'Transaction Delay', N'Log Bytes Flushed/sec', N'Transactions/sec')
		-- TO DO select all perfcounters for all databases so that you only need to access them once.
		select @role = (mirroring_role - 1),
			@status = mirroring_state,
			@witness_status = mirroring_witness_state,
			@failover_lsn = mirroring_failover_lsn,
			@end_of_log_lsn = mirroring_end_of_log_lsn
				from sys.database_mirroring where database_id = @database_id
		-- TO DO: when doing the join, store the database id.
		select @log_flush_rate = cntr_value from @perfcounters where counter_name = N'Log Bytes Flushed/sec'
		select @send_queue_size = cntr_value from @perfcounters where counter_name = N'Log Send Queue KB'
		select @send_rate = cntr_value from @perfcounters where counter_name = N'Log Bytes Sent/sec'
		select @redo_queue_size = cntr_value from @perfcounters where counter_name = N'Redo Queue KB'
		select @redo_rate = cntr_value from @perfcounters where counter_name = N'Redo Bytes/sec'
		select @transaction_delay = cntr_value from @perfcounters where counter_name = N'Transaction Delay'
		select @transactions_per_sec = cntr_value from @perfcounters where counter_name = N'Transactions/sec'
		set @time = getutcdate()
		set @local_time = getdate()

-- 4. and insert it here
		insert into msdb.dbo.dbm_monitor_data (database_id, role, status, witness_status, failover_lsn, end_of_log_lsn, log_flush_rate,
				send_queue_size, send_rate, redo_queue_size, redo_rate, transaction_delay, transactions_per_sec, time, local_time)
		values( @database_id, @role, @status, @witness_status, @failover_lsn, @end_of_log_lsn, @log_flush_rate, @send_queue_size, @send_rate,
			@redo_queue_size, @redo_rate, @transaction_delay, @transactions_per_sec, @time, @local_time )

		--
		-- Raise the alerts (as errors)
		--
		--
		-- we need to call sys.sp_dbmmonitorresults to get the last row inserted and then we will compare those results with what is in the alerts table
		--
		declare	@alert		bit,
				@threshold	int,
				@command	char(256),
				@time_behind_alert_value	datetime,
				@send_queue_alert_value		int,
				@redo_queue_alert_value		int,
				@average_delay_alert_value	int,
				@temp_time					int

		declare @results table(
			database_name			sysname,	-- Name of database
			role					int,		-- 1 = Principal, 2 = Mirror
			mirroring_state			int,		-- 0 = Suspended, 1 = Disconnected, 2 = Synchronizing, 3 = Pending Failover, 4 = Synchronized
			witness_status			int,		-- 1 = Connected, 2 = Disconnected
			log_generation_rate		int NULL,	-- in kb / sec
			unsent_log				int,		-- in kb
			send_rate				int NULL,	-- in kb / sec
			unrestored_log			int,		-- in kb
			recovery_rate			int NULL,	-- in kb / sec
			transaction_delay		int NULL,	-- in ms
			transactions_per_sec	int NULL,	-- in trans / sec
			average_delay			int,		-- in ms
			time_recorded			datetime,
			time_behind				datetime,
			local_time				datetime
		)

		set @command = N'sys.sp_dbmmonitorresults ''' + replace(@database_name, N'''',N'''''') + N''',0,0'
		-- get just the values we want to test
		insert into @results exec (@command)
		select @time_behind_alert_value = time_behind, @send_queue_alert_value = unsent_log,
				@redo_queue_alert_value = unrestored_log, @average_delay_alert_value = average_delay
		from @results where database_name = @database_name

		-- These next four code blocks are the same:
		--	If the alert is enabled AND the value is above the threshold, fire the event
		-- The four code blocks are time behind, send queue, redo queue and average delay.

		-- time behind
		set @alert = 0	-- from SteveLi.  This will make sure that if there are problems with the select, the alert
						-- will not accidentally fire.
		select @threshold = time_behind, @alert = enable_time_behind
			from msdb.dbo.dbm_monitor_alerts where database_id = @database_id
		begin
			set @temp_time = datediff(minute, @time_behind_alert_value, getutcdate())
			if ( @alert = 1 and @threshold < @temp_time ) -- time_behind_alert_value is datetime
			begin
				raiserror( 32040, 10, 1, @temp_time, @threshold ) with log
			end
		end
		-- send queue
		set @alert = 0
		select @threshold = send_queue, @alert = enable_send_queue
			from msdb.dbo.dbm_monitor_alerts where database_id = @database_id
		begin
			if ( @alert = 1 and @threshold < @send_queue_alert_value )
			begin
				raiserror( 32042, 10, 2, @send_queue_alert_value, @threshold ) with log
			end
		end
		-- redo queue
		set @alert = 0
		select @threshold = redo_queue, @alert = enable_redo_queue
			from msdb.dbo.dbm_monitor_alerts where database_id = @database_id
		begin
			if ( @alert = 1 and @threshold < @redo_queue_alert_value )
			begin
				raiserror( 32043, 10, 3, @redo_queue_alert_value, @threshold ) with log
			end
		end
		-- average delay
		set @alert = 0
		select @threshold = average_delay, @alert = enable_average_delay
			from msdb.dbo.dbm_monitor_alerts where database_id = @database_id
		begin
			if ( @alert = 1 and @threshold < @average_delay_alert_value )
			begin
				raiserror( 32044, 10, 4, @average_delay_alert_value, @threshold ) with log
			end
		end

		-- Prune the Data Table.
		select @retention_period = retention_period from msdb.dbo.dbm_monitor_alerts where database_id = @database_id
		if @retention_period is null
			set @retention_period = 168 -- 168 hours is equivalent to 7 days
		set @oldest_date = getutcdate() - (@retention_period / 24.)
		delete from msdb.dbo.dbm_monitor_data where time < @oldest_date and database_id = @database_id
	end
	-- OK, this SP was called with no database specified.
	-- We are going to go through all the databases that are mirrored and update them.
	else
	begin
		declare dbmCursor cursor local scroll
			for select
				database_id
			from sys.database_mirroring
			where mirroring_guid is not null

		open dbmCursor
		fetch next from dbmCursor
			into @database_id

		while @@fetch_status=0
		begin
-- Better make sure sys.sp_dbmmonitorupdate with a null parameter.  Could cause real bad problems.
			set @database_name = db_name( @database_id )
			if @database_name is not null
			begin

				exec sys.sp_dbmmonitorupdate @database_name
				fetch next from dbmCursor
					into @database_id
			end
		end

		close dbmCursor
		deallocate dbmCursor
	end

return 0
end


/*====  SQL Server 2022 CU20 GDR version  ====*/

create procedure sys.sp_dbmmonitorupdate
(
	@database_name		sysname = null	-- if null update all mirrored databases
)
as
begin
	set nocount on
	if (is_srvrolemember(N'sysadmin') <> 1 )
    begin
		raiserror(21089, 16, 1)
		return 1
	end
	if ( db_name() != N'msdb' )
	begin
		raiserror(32045, 16, 1, N'sys.sp_dbmmonitorupdate')
		return 1
	end

	declare		@retcode	int

	if object_id ( N'msdb.dbo.dbm_monitor_data', N'U' ) is null
	begin
		create table msdb.dbo.dbm_monitor_data (		-- go through the code to see if there SHOULD be nulls.
			database_id				smallint,
			role					bit null,
			status					tinyint null,
			witness_status			tinyint null,
			log_flush_rate			bigint null,
			send_queue_size			bigint null,
			send_rate				bigint null,
			redo_queue_size			bigint null,
			redo_rate				bigint null,
			transaction_delay		bigint null,
			transactions_per_sec	bigint null,
			time					datetime,
			end_of_log_lsn			numeric(25,0),
			failover_lsn			numeric(25,0),
			local_time				datetime
		)

		exec @retcode = msdb.dbo.sp_MS_marksystemobject dbm_monitor_data
		if ( @@error != 0 OR @retcode != 0 )
		begin
			raiserror( 32038, 16, 1 )
			return 1
		end

		create clustered index dbmmonitor1
            on msdb.dbo.dbm_monitor_data (database_id asc, time asc )
	end

	-- TO DO: create some keys depending on enterences.
	-- TO DO: make sure that the rows are unique
	if object_id ( N'msdb.dbo.dbm_monitor_alerts', N'U' ) is null
	begin
		create table msdb.dbo.dbm_monitor_alerts (
			database_id				smallint,
			retention_period		int null,	--this defaults to 7 days.  checked during the table update
			time_behind				int null,
			enable_time_behind		bit null,
			send_queue				int null,
			enable_send_queue		bit null,
			redo_queue				int null,
			enable_redo_queue		bit null,
			average_delay			int null,
			enable_average_delay	bit null
		)

		exec @retcode = msdb.dbo.sp_MS_marksystemobject dbm_monitor_alerts
		if ( @@error != 0 OR @retcode != 0 )
		begin
			raiserror( 32038, 16, 2 )
			return 1
		end
	end

	if ( select name from sys.database_principals where name = N'dbm_monitor') is null
	begin
		create role dbm_monitor
		grant select on object::msdb.dbo.dbm_monitor_data to dbm_monitor
	end

	if @database_name is not null
	begin
		--
		-- Check if the database specified exists
		--
		if not exists (select * from master.sys.databases where name = @database_name)
		begin
			raiserror(15010, 16, 1, @database_name)
			return 1
		end
		--
		-- Check to see if it is mirrored
		--
		if (select mirroring_guid from master.sys.database_mirroring where database_id = db_id(@database_name)) is null
		begin
			raiserror(32039, 16, 1, @database_name)
			return 1
		end

	declare
		@database_id			smallint,
		@role					bit,
		@status					tinyint,
		@witness_status			tinyint,
		@log_flush_rate			bigint ,
		@send_queue_size		bigint ,
		@send_rate				bigint ,
		@redo_queue_size		bigint ,
		@redo_rate				bigint ,
		@transaction_delay		bigint ,
		@transactions_per_sec	bigint ,
		@time					datetime ,
		@end_of_log_lsn			numeric(25,0),
		@failover_lsn			numeric(25,0),
		@local_time				datetime

	declare
		@retention_period		int,
		@oldest_date			datetime

		set @database_id = DB_ID( @database_name )

-- To select the correct perf counter, we need the instance name.
		declare
			@perf_instance1		nvarchar(256),
			@perf_instance2		nvarchar(256),
			@instance			nvarchar(128)

		select @instance = convert( nvarchar,  serverproperty(N'instancename'))
		if @instance is null
		begin
			set @instance = N'SQLServer'
		end
		else
		begin
			set @instance = N'MSSQL$' + @instance
		end

		set @perf_instance1 = left(@instance, len(@instance)) + N':Database Mirroring'
		set @perf_instance2 = left(@instance, len(@instance)) + N':Databases'

--
-- Insert a single row in the table for each database
--
-- 1. Pull out the perf counters
-- 2. Pull out the information from sys.database_mirroring
-- 3. Get the end of log lsn

		declare @perfcounters table(
			counter_name		nchar(128),
			cntr_value			bigint
		)

		insert into @perfcounters select counter_name, cntr_value from sys.dm_os_performance_counters where
			(object_name = @perf_instance1 or object_name = @perf_instance2 ) and
			instance_name = @database_name and
			counter_name IN (N'Log Send Queue KB', N'Log Bytes Sent/sec', N'Redo Queue KB', N'Redo Bytes/sec', N'Transaction Delay', N'Log Bytes Flushed/sec', N'Transactions/sec')
		-- TO DO select all perfcounters for all databases so that you only need to access them once.
		select @role = (mirroring_role - 1),
			@status = mirroring_state,
			@witness_status = mirroring_witness_state,
			@failover_lsn = mirroring_failover_lsn,
			@end_of_log_lsn = mirroring_end_of_log_lsn
				from sys.database_mirroring where database_id = @database_id
		-- TO DO: when doing the join, store the database id.
		select @log_flush_rate = cntr_value from @perfcounters where counter_name = N'Log Bytes Flushed/sec'
		select @send_queue_size = cntr_value from @perfcounters where counter_name = N'Log Send Queue KB'
		select @send_rate = cntr_value from @perfcounters where counter_name = N'Log Bytes Sent/sec'
		select @redo_queue_size = cntr_value from @perfcounters where counter_name = N'Redo Queue KB'
		select @redo_rate = cntr_value from @perfcounters where counter_name = N'Redo Bytes/sec'
		select @transaction_delay = cntr_value from @perfcounters where counter_name = N'Transaction Delay'
		select @transactions_per_sec = cntr_value from @perfcounters where counter_name = N'Transactions/sec'
		set @time = getutcdate()
		set @local_time = getdate()

-- 4. and insert it here
		insert into msdb.dbo.dbm_monitor_data (database_id, role, status, witness_status, failover_lsn, end_of_log_lsn, log_flush_rate,
				send_queue_size, send_rate, redo_queue_size, redo_rate, transaction_delay, transactions_per_sec, time, local_time)
		values( @database_id, @role, @status, @witness_status, @failover_lsn, @end_of_log_lsn, @log_flush_rate, @send_queue_size, @send_rate,
			@redo_queue_size, @redo_rate, @transaction_delay, @transactions_per_sec, @time, @local_time )

		--
		-- Raise the alerts (as errors)
		--
		--
		-- we need to call sys.sp_dbmmonitorresults to get the last row inserted and then we will compare those results with what is in the alerts table
		--
		declare	@alert		bit,
				@threshold	int,
				@command	nvarchar(4000),
				@time_behind_alert_value	datetime,
				@send_queue_alert_value		int,
				@redo_queue_alert_value		int,
				@average_delay_alert_value	int,
				@temp_time					int

		declare @results table(
			database_name			sysname,	-- Name of database
			role					int,		-- 1 = Principal, 2 = Mirror
			mirroring_state			int,		-- 0 = Suspended, 1 = Disconnected, 2 = Synchronizing, 3 = Pending Failover, 4 = Synchronized
			witness_status			int,		-- 1 = Connected, 2 = Disconnected
			log_generation_rate		int NULL,	-- in kb / sec
			unsent_log				int,		-- in kb
			send_rate				int NULL,	-- in kb / sec
			unrestored_log			int,		-- in kb
			recovery_rate			int NULL,	-- in kb / sec
			transaction_delay		int NULL,	-- in ms
			transactions_per_sec	int NULL,	-- in trans / sec
			average_delay			int,		-- in ms
			time_recorded			datetime,
			time_behind				datetime,
			local_time				datetime
		)

		set @command = N'sys.sp_dbmmonitorresults ''' + replace(@database_name, N'''',N'''''') + N''',0,0'
		-- get just the values we want to test
		insert into @results exec (@command)
		select @time_behind_alert_value = time_behind, @send_queue_alert_value = unsent_log,
				@redo_queue_alert_value = unrestored_log, @average_delay_alert_value = average_delay
		from @results where database_name = @database_name

		-- These next four code blocks are the same:
		--	If the alert is enabled AND the value is above the threshold, fire the event
		-- The four code blocks are time behind, send queue, redo queue and average delay.

		-- time behind
		set @alert = 0	-- from SteveLi.  This will make sure that if there are problems with the select, the alert
						-- will not accidentally fire.
		select @threshold = time_behind, @alert = enable_time_behind
			from msdb.dbo.dbm_monitor_alerts where database_id = @database_id
		begin
			set @temp_time = datediff(minute, @time_behind_alert_value, getutcdate())
			if ( @alert = 1 and @threshold < @temp_time ) -- time_behind_alert_value is datetime
			begin
				raiserror( 32040, 10, 1, @temp_time, @threshold ) with log
			end
		end
		-- send queue
		set @alert = 0
		select @threshold = send_queue, @alert = enable_send_queue
			from msdb.dbo.dbm_monitor_alerts where database_id = @database_id
		begin
			if ( @alert = 1 and @threshold < @send_queue_alert_value )
			begin
				raiserror( 32042, 10, 2, @send_queue_alert_value, @threshold ) with log
			end
		end
		-- redo queue
		set @alert = 0
		select @threshold = redo_queue, @alert = enable_redo_queue
			from msdb.dbo.dbm_monitor_alerts where database_id = @database_id
		begin
			if ( @alert = 1 and @threshold < @redo_queue_alert_value )
			begin
				raiserror( 32043, 10, 3, @redo_queue_alert_value, @threshold ) with log
			end
		end
		-- average delay
		set @alert = 0
		select @threshold = average_delay, @alert = enable_average_delay
			from msdb.dbo.dbm_monitor_alerts where database_id = @database_id
		begin
			if ( @alert = 1 and @threshold < @average_delay_alert_value )
			begin
				raiserror( 32044, 10, 4, @average_delay_alert_value, @threshold ) with log
			end
		end

		-- Prune the Data Table.
		select @retention_period = retention_period from msdb.dbo.dbm_monitor_alerts where database_id = @database_id
		if @retention_period is null
			set @retention_period = 168 -- 168 hours is equivalent to 7 days
		set @oldest_date = getutcdate() - (@retention_period / 24.)
		delete from msdb.dbo.dbm_monitor_data where time < @oldest_date and database_id = @database_id
	end
	-- OK, this SP was called with no database specified.
	-- We are going to go through all the databases that are mirrored and update them.
	else
	begin
		declare dbmCursor cursor local scroll
			for select
				database_id
			from sys.database_mirroring
			where mirroring_guid is not null

		open dbmCursor
		fetch next from dbmCursor
			into @database_id

		while @@fetch_status=0
		begin
-- Better make sure sys.sp_dbmmonitorupdate with a null parameter.  Could cause real bad problems.
			set @database_name = db_name( @database_id )
			if @database_name is not null
			begin

				exec sys.sp_dbmmonitorupdate @database_name
				fetch next from dbmCursor
					into @database_id
			end
		end

		close dbmCursor
		deallocate dbmCursor
	end

return 0
end

