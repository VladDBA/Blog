SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/

create procedure sys.sp_updatestats
	@resample char(8)='NO'
as
	
	declare @dbsid varbinary(85)
	
	select @dbsid = owner_sid
		from sys.databases
		where name = db_name()

	-- Check the user sysadmin, dbo or member of db_owner when the engineedition is 5 (Azure SQL DB) or 12 (TridentNative)
	if not is_srvrolemember('sysadmin') = 1 and suser_sid() <> @dbsid and not (is_rolemember('db_owner') = 1 and SERVERPROPERTY('engineedition') in (5,12))
	begin
		raiserror(15247,-1,-1)
		return (1)
	end
	-- cannot execute against R/O databases  
	if DATABASEPROPERTYEX(db_name(), 'Updateability')=N'READ_ONLY'
	begin
		raiserror(15635,-1,-1,N'sp_updatestats')
		return (1)
	end

	if upper(@resample)<>'RESAMPLE' and upper(@resample)<>'NO'
	begin
		raiserror(14138, -1, -1, @resample)
		return (1)
	end

	-- required so it can update stats on ICC/IVs
	set ansi_warnings on
	set ansi_padding on
	set arithabort on
	set concat_null_yields_null on
	set numeric_roundabort off

	declare @exec_stmt nvarchar(4000)		-- "UPDATE STATISTICS [sysname].[sysname] [sysname] WITH RESAMPLE NORECOMPUTE"
	declare @exec_stmt_head nvarchar(4000)	-- "UPDATE STATISTICS [sysname].[sysname] "
	declare @options nvarchar(100)			-- "RESAMPLE NORECOMPUTE"

	declare @index_names cursor

	declare @ind_name sysname
	declare @ind_id int
	declare @ind_rowmodctr int
	declare @updated_count int
	declare @skipped_count int

	declare @sch_id int
	declare @schema_name sysname
	declare @table_name sysname
	declare @table_id int
	declare @table_type char(2)
	declare @schema_table_name nvarchar(640) -- assuming sysname is 128 chars, 5x that, so it's > 128*4+4

	declare @compatlvl tinyint

	declare ms_crs_tnames cursor local fast_forward read_only for
		select name, object_id, schema_id, type from sys.objects o
		where o.type = 'U' or o.type = 'IT'
	open ms_crs_tnames
	fetch next from ms_crs_tnames into @table_name, @table_id, @sch_id, @table_type

	-- determine compatibility level
	select @compatlvl = cmptlevel from sys.sysdatabases where name = db_name()

	while (@@fetch_status <> -1) -- fetch successful
	begin
		-- generate fully qualified quoted name
		select @schema_name = schema_name(@sch_id)
		select @schema_table_name = quotename(@schema_name, '[') +'.'+ quotename(rtrim(@table_name), '[')

		-- check for table with disabled clustered index
		if (1 = isnull((select is_disabled from sys.indexes where object_id = @table_id and index_id = 1), 0))
		begin
			-- raiserror('Table ''%s'': cannot perform the operation on the table because its clustered index is disabled', -1, -1, @tablename)
			raiserror(15654, -1, -1, @schema_table_name)
		end
		else
		begin
			-- filter out local temp tables and PVS tables
			if ((@@fetch_status <> -2) and 
				(substring(@table_name, 1, 1) <> '#') and
				(@schema_name <> 'sys' or @table_name <> 'persistent_version_store') and  -- PVS tables
				(@schema_name <> 'sys' or @table_name <> 'persistent_version_store_long_term')) -- PVS tables
			begin
				-- reset counters for this table
				select @updated_count = 0
				select @skipped_count = 0

				-- print status message
				--raiserror('Updating %s', -1, -1, @schema_table_name)
				raiserror(15650, -1, -1, @schema_table_name)

				-- initial statement preparation: UPDATE STATISTICS [schema].[name]
				select @exec_stmt_head = 'UPDATE STATISTICS ' + @schema_table_name + ' '

				-- using another cursor to iterate through
				-- indices and stats (user and auto-created)
				-- Hekaton indexes do not appear in sys.sysindexes so we need to use sys.stats instead
				-- Note that OBJECTPROPERTY returns NULL on type="IT" tables, thus we only call it on type='U' tables
				if ((@table_type = 'U') and (1 = OBJECTPROPERTY(@table_id, 'TableIsMemoryOptimized')))	-- Hekaton tables
				begin
					set @index_names = cursor local fast_forward read_only for
						select name, stat.stats_id, modification_counter as rowmodctr
						from sys.stats as stat
						cross apply sys.dm_db_stats_properties(stat.object_id, stat.stats_id)
						where stat.object_id = @table_id and indexproperty(stat.object_id, name, 'ishypothetical') = 0
						and indexproperty(stat.object_id, name, 'iscolumnstore') = 0 -- columnstore indexes cannot update stats.
						order by stat.stats_id
				end
				else
				begin
					set @index_names = cursor local fast_forward read_only for
						select name, indid, rowmodctr from sys.sysindexes
						where id = @table_id and indid > 0 and indexproperty(id, name, 'ishypothetical') = 0 
						and indexproperty(id, name, 'iscolumnstore') = 0
						order by indid
				end

				open @index_names
				fetch @index_names into @ind_name, @ind_id, @ind_rowmodctr

				-- if there are no stats, skip update
				if @@fetch_status < 0
					--raiserror('    %d indexes/statistics have been updated, %d did not require update.', -1, -1, @updated_count, @skipped_count)
					raiserror(15651, -1, -1, @updated_count, @skipped_count)
				else 
				begin
					while @@fetch_status >= 0
					begin
						-- create quoted index name
						declare @ind_name_quoted nvarchar(258)
						select @ind_name_quoted = quotename(@ind_name, '[')

						-- reset options
						select @options = ''

						declare @is_ver_current bit
						select @is_ver_current = stats_ver_current(@table_id, @ind_id)

						-- note that <> 0 should work against old and new rowmodctr logic (when it is always > 0)
						-- also, force a refresh if the stats blob version is not current
						if ((@ind_rowmodctr is null) or (@ind_rowmodctr <> 0) or ((@is_ver_current is not null) and (@is_ver_current = 0)))
						begin
							select @exec_stmt = @exec_stmt_head + @ind_name_quoted

							-- Add FULLSCAN for hekaton tables if DB compatlvl < 130
							-- Note that OBJECTPROPERTY returns NULL on type="IT" tables, thus we only call it on type='U' tables
							if ((@compatlvl < 130) and (@table_type = 'U') and (1 = OBJECTPROPERTY(@table_id, 'TableIsMemoryOptimized')))	-- Hekaton tables
								select @options = 'FULLSCAN'

							-- add resample if needed
							else if (upper(@resample)='RESAMPLE')
								select @options = 'RESAMPLE '
							
							if (@compatlvl >= 90)
								-- put norecompute if local properties are set to AUTOSTATS = OFF
								-- note that ind name is unique within the object
								if ((select no_recompute from sys.stats where object_id = @table_id and name = @ind_name) = 1)
								begin
									if (len(@options) > 0) select @options = @options + ', NORECOMPUTE'
									else select @options = 'NORECOMPUTE'
								end

							if (len(@options) > 0)
								select @exec_stmt = @exec_stmt + ' WITH ' + @options

							--print @exec_stmt
							exec (@exec_stmt)
							--raiserror('    %s has been updated...', -1, -1, @ind_name_quoted)
							raiserror(15652, -1, -1, @ind_name_quoted)
							select @updated_count = @updated_count + 1
						end
						else
						begin
							--raiserror('    %s, update is not necessary...', -1, -1, @ind_name_quoted)
							raiserror(15653, -1, -1, @ind_name_quoted)
							select @skipped_count = @skipped_count + 1
						end
						fetch @index_names into @ind_name, @ind_id, @ind_rowmodctr
					end
					--raiserror('    %d index(es)/statistic(s) have been updated, %d did not require update/disabled.', -1, -1, @updated_count, @skipped_count)
					raiserror(15651, -1, -1, @updated_count, @skipped_count)
				end
				deallocate @index_names
			end
		end
		print ' '
		fetch next from ms_crs_tnames into @table_name, @table_id, @sch_id, @table_type
	end
	raiserror(15005,-1,-1)
	deallocate ms_crs_tnames
	return(0) -- sp_updatestats


/*====  SQL Server 2022 version  ====*/

create procedure sys.sp_updatestats
	@resample char(8)='NO'
as
	
	declare @dbsid varbinary(85)
	
	select @dbsid = owner_sid
		from sys.databases
		where name = db_name()

	-- Check the user sysadmin, dbo or member of db_owner when the engineedition is 5 which is the edition for SQL Azure
	if not is_srvrolemember('sysadmin') = 1 and suser_sid() <> @dbsid and not (is_rolemember('db_owner') = 1 and SERVERPROPERTY('engineedition') = 5)
	begin
		raiserror(15247,-1,-1)
		return (1)
	end
	-- cannot execute against R/O databases  
	if DATABASEPROPERTYEX(db_name(), 'Updateability')=N'READ_ONLY'
	begin
		raiserror(15635,-1,-1,N'sp_updatestats')
		return (1)
	end

	if upper(@resample)<>'RESAMPLE' and upper(@resample)<>'NO'
	begin
		raiserror(14138, -1, -1, @resample)
		return (1)
	end

	-- required so it can update stats on ICC/IVs
	set ansi_warnings on
	set ansi_padding on
	set arithabort on
	set concat_null_yields_null on
	set numeric_roundabort off

	declare @exec_stmt nvarchar(4000)		-- "UPDATE STATISTICS [sysname].[sysname] [sysname] WITH RESAMPLE NORECOMPUTE"
	declare @exec_stmt_head nvarchar(4000)	-- "UPDATE STATISTICS [sysname].[sysname] "
	declare @options nvarchar(100)			-- "RESAMPLE NORECOMPUTE"

	declare @index_names cursor

	declare @ind_name sysname
	declare @ind_id int
	declare @ind_rowmodctr int
	declare @updated_count int
	declare @skipped_count int

	declare @sch_id int
	declare @schema_name sysname
	declare @table_name sysname
	declare @table_id int
	declare @table_type char(2)
	declare @schema_table_name nvarchar(640) -- assuming sysname is 128 chars, 5x that, so it's > 128*4+4

	declare @compatlvl tinyint

	declare ms_crs_tnames cursor local fast_forward read_only for
		select name, object_id, schema_id, type from sys.objects o
		where o.type = 'U' or o.type = 'IT'
	open ms_crs_tnames
	fetch next from ms_crs_tnames into @table_name, @table_id, @sch_id, @table_type

	-- determine compatibility level
	select @compatlvl = cmptlevel from sys.sysdatabases where name = db_name()

	while (@@fetch_status <> -1) -- fetch successful
	begin
		-- generate fully qualified quoted name
		select @schema_name = schema_name(@sch_id)
		select @schema_table_name = quotename(@schema_name, '[') +'.'+ quotename(rtrim(@table_name), '[')

		-- check for table with disabled clustered index
		if (1 = isnull((select is_disabled from sys.indexes where object_id = @table_id and index_id = 1), 0))
		begin
			-- raiserror('Table ''%s'': cannot perform the operation on the table because its clustered index is disabled', -1, -1, @tablename)
			raiserror(15654, -1, -1, @schema_table_name)
		end
		else
		begin
			-- filter out local temp tables and PVS tables
			if ((@@fetch_status <> -2) and 
				(substring(@table_name, 1, 1) <> '#') and
				(@schema_name <> 'sys' or @table_name <> 'persistent_version_store') and  -- PVS tables
				(@schema_name <> 'sys' or @table_name <> 'persistent_version_store_long_term')) -- PVS tables
			begin
				-- reset counters for this table
				select @updated_count = 0
				select @skipped_count = 0

				-- print status message
				--raiserror('Updating %s', -1, -1, @schema_table_name)
				raiserror(15650, -1, -1, @schema_table_name)

				-- initial statement preparation: UPDATE STATISTICS [schema].[name]
				select @exec_stmt_head = 'UPDATE STATISTICS ' + @schema_table_name + ' '

				-- using another cursor to iterate through
				-- indices and stats (user and auto-created)
				-- Hekaton indexes do not appear in sys.sysindexes so we need to use sys.stats instead
				-- Note that OBJECTPROPERTY returns NULL on type="IT" tables, thus we only call it on type='U' tables
				if ((@table_type = 'U') and (1 = OBJECTPROPERTY(@table_id, 'TableIsMemoryOptimized')))	-- Hekaton tables
				begin
					set @index_names = cursor local fast_forward read_only for
						select name, stat.stats_id, modification_counter as rowmodctr
						from sys.stats as stat
						cross apply sys.dm_db_stats_properties(stat.object_id, stat.stats_id)
						where stat.object_id = @table_id and indexproperty(stat.object_id, name, 'ishypothetical') = 0
						and indexproperty(stat.object_id, name, 'iscolumnstore') = 0 -- columnstore indexes cannot update stats.
						order by stat.stats_id
				end
				else
				begin
					set @index_names = cursor local fast_forward read_only for
						select name, indid, rowmodctr from sys.sysindexes
						where id = @table_id and indid > 0 and indexproperty(id, name, 'ishypothetical') = 0 
						and indexproperty(id, name, 'iscolumnstore') = 0
						order by indid
				end

				open @index_names
				fetch @index_names into @ind_name, @ind_id, @ind_rowmodctr

				-- if there are no stats, skip update
				if @@fetch_status < 0
					--raiserror('    %d indexes/statistics have been updated, %d did not require update.', -1, -1, @updated_count, @skipped_count)
					raiserror(15651, -1, -1, @updated_count, @skipped_count)
				else 
				begin
					while @@fetch_status >= 0
					begin
						-- create quoted index name
						declare @ind_name_quoted nvarchar(258)
						select @ind_name_quoted = quotename(@ind_name, '[')

						-- reset options
						select @options = ''

						declare @is_ver_current bit
						select @is_ver_current = stats_ver_current(@table_id, @ind_id)

						-- note that <> 0 should work against old and new rowmodctr logic (when it is always > 0)
						-- also, force a refresh if the stats blob version is not current
						if ((@ind_rowmodctr is null) or (@ind_rowmodctr <> 0) or ((@is_ver_current is not null) and (@is_ver_current = 0)))
						begin
							select @exec_stmt = @exec_stmt_head + @ind_name_quoted

							-- Add FULLSCAN for hekaton tables if DB compatlvl < 130
							-- Note that OBJECTPROPERTY returns NULL on type="IT" tables, thus we only call it on type='U' tables
							if ((@compatlvl < 130) and (@table_type = 'U') and (1 = OBJECTPROPERTY(@table_id, 'TableIsMemoryOptimized')))	-- Hekaton tables
								select @options = 'FULLSCAN'

							-- add resample if needed
							else if (upper(@resample)='RESAMPLE')
								select @options = 'RESAMPLE '
							
							if (@compatlvl >= 90)
								-- put norecompute if local properties are set to AUTOSTATS = OFF
								-- note that ind name is unique within the object
								if ((select no_recompute from sys.stats where object_id = @table_id and name = @ind_name) = 1)
								begin
									if (len(@options) > 0) select @options = @options + ', NORECOMPUTE'
									else select @options = 'NORECOMPUTE'
								end

							if (len(@options) > 0)
								select @exec_stmt = @exec_stmt + ' WITH ' + @options

							--print @exec_stmt
							exec (@exec_stmt)
							--raiserror('    %s has been updated...', -1, -1, @ind_name_quoted)
							raiserror(15652, -1, -1, @ind_name_quoted)
							select @updated_count = @updated_count + 1
						end
						else
						begin
							--raiserror('    %s, update is not necessary...', -1, -1, @ind_name_quoted)
							raiserror(15653, -1, -1, @ind_name_quoted)
							select @skipped_count = @skipped_count + 1
						end
						fetch @index_names into @ind_name, @ind_id, @ind_rowmodctr
					end
					--raiserror('    %d index(es)/statistic(s) have been updated, %d did not require update/disabled.', -1, -1, @updated_count, @skipped_count)
					raiserror(15651, -1, -1, @updated_count, @skipped_count)
				end
				deallocate @index_names
			end
		end
		print ' '
		fetch next from ms_crs_tnames into @table_name, @table_id, @sch_id, @table_type
	end
	raiserror(15005,-1,-1)
	deallocate ms_crs_tnames
	return(0) -- sp_updatestats

