SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
/*====  SQL Server 2025 version  ====*/

create procedure sys.sp_helpindex
	@objname nvarchar(776)		-- the table to check for indexes
as
	-- PRELIM
	set nocount on

	declare @objid int,			-- the object id of the table
			@indid smallint,	-- the index id of an index
			@groupid int,  		-- the filegroup id of an index
			@indname sysname,
			@groupname sysname,
			@status int,
			@keys nvarchar(2126),	--Length (16*max_identifierLength)+(15*2)+(16*3)
			@dbname	sysname,
			@ignore_dup_key	bit,
			@is_unique		bit,
			@is_hypothetical	bit,
			@is_primary_key	bit,
			@is_unique_key 	bit,
			@is_columnstore	bit,
			@auto_created	bit,
			@no_recompute	bit,
			@memory_optimized bit, -- For hekaton tables
			@hash_index		bit -- The index is a hash index

	-- Check to see that the object names are local to the current database.
	select @dbname = parsename(@objname,3)
	if @dbname is null
		select @dbname = db_name()
	else if @dbname <> db_name()
		begin
			raiserror(15250,-1,-1)
			return (1)
		end

	-- Check to see that the table exists and initialize @objid.
	select @objid = object_id(@objname)
	if @objid is NULL
	begin
		raiserror(15009,-1,-1,@objname,@dbname)
		return (1)
	end

	-- OPEN CURSOR OVER INDEXES (skip stats: bug shiloh_51196)
	declare ms_crs_ind cursor local static for
		select i.index_id, i.data_space_id, i.name,
			i.ignore_dup_key, i.is_unique, i.is_hypothetical, i.is_primary_key, i.is_unique_constraint,
			case when (type = 5 or type = 6) then 1 else 0 end,
			case when (type = 5 or type = 6) then 0 else s.auto_created end,
			case when (type = 5 or type = 6) then 0 else s.no_recompute end,
			case when (type = 7) then 1 else 0 end
		from sys.indexes i left join sys.stats s
			on i.object_id = s.object_id and i.index_id = s.stats_id
		where i.object_id = @objid and type in (1, 2, 5, 6, 7)
	open ms_crs_ind
	fetch ms_crs_ind into @indid, @groupid, @indname, @ignore_dup_key, @is_unique, @is_hypothetical,
			@is_primary_key, @is_unique_key, @is_columnstore, @auto_created, @no_recompute, @hash_index

	-- IF NO INDEX, QUIT
	if @@fetch_status < 0
	begin
		deallocate ms_crs_ind
		raiserror(15472,-1,-1,@objname) -- Object does not have any indexes.
		return (0)
	end

	-- create temp table
	CREATE TABLE #spindtab
	(
		index_name			sysname	collate catalog_default NOT NULL,
		index_id				int,
		ignore_dup_key		bit,
		is_unique				bit,
		is_hypothetical		bit,
		is_primary_key		bit,
		is_unique_key			bit,
		is_columnstore		bit,
		auto_created			bit,
		no_recompute			bit,
		memory_optimized		bit,
		hash_index			bit,
		groupname			sysname collate catalog_default NULL,
		index_keys			nvarchar(2126)	collate catalog_default NULL -- see @keys above for length descr
	)

	-- Now check out each index, figure out its type and keys and
	--	save the info in a temporary table that we'll print out at the end.
	while @@fetch_status >= 0
	begin
		-- First we'll figure out what the keys are.
		declare @i int, @thiskey nvarchar(131) -- 128+3

		select @keys = index_col(@objname, @indid, 1), @i = 2
		if (indexkey_property(@objid, @indid, 1, 'isdescending') = 1)
			select @keys = @keys  + '(-)'

		select @thiskey = index_col(@objname, @indid, @i)
		if ((@thiskey is not null) and (indexkey_property(@objid, @indid, @i, 'isdescending') = 1))
			select @thiskey = @thiskey + '(-)'

		while (@thiskey is not null )
		begin
			select @keys = @keys + ', ' + @thiskey, @i = @i + 1
			select @thiskey = index_col(@objname, @indid, @i)
			if ((@thiskey is not null) and (indexkey_property(@objid, @indid, @i, 'isdescending') = 1))
				select @thiskey = @thiskey + '(-)'
		end

		select @groupname = null
		if (serverproperty('EngineEdition') not in (5,12))
		select @groupname = name from sys.data_spaces where data_space_id = @groupid

		select @memory_optimized = is_memory_optimized from sys.tables where object_id = @objid

		-- INSERT ROW FOR INDEX
		insert into #spindtab values (@indname, @indid, @ignore_dup_key, @is_unique, @is_hypothetical,
			@is_primary_key, @is_unique_key, @is_columnstore, @auto_created, @no_recompute, @memory_optimized, @hash_index, @groupname, @keys)

		-- Next index
		fetch ms_crs_ind into @indid, @groupid, @indname, @ignore_dup_key, @is_unique, @is_hypothetical,
			@is_primary_key, @is_unique_key, @is_columnstore, @auto_created, @no_recompute, @hash_index
	end
	deallocate ms_crs_ind

	-- DISPLAY THE RESULTS
	select
		'index_name' = index_name,
		'index_description' = convert(varchar(210), --bits 16 off, 1, 2, 16777216 on, located on group
				case when index_id = 1 then 'clustered' else 'nonclustered' end
				+ case when hash_index = 1 then ' hash' else '' end
				+ case when ignore_dup_key <>0 then ', ignore duplicate keys' else '' end
				+ case when is_unique <>0 then ', unique' else '' end
				+ case when is_hypothetical <>0 then ', hypothetical' else '' end
				+ case when is_primary_key <>0 then ', primary key' else '' end
				+ case when is_unique_key <>0 then ', unique key' else '' end
				+ case when is_columnstore <>0 then ', columnstore' else '' end
				+ case when auto_created <>0 then ', auto create' else '' end
				+ case when no_recompute <>0 then ', stats no recompute' else '' end
				+ case when memory_optimized = 1 then ' located in MEMORY ' else '' end 
				+ case when groupname IS NOT NULL AND (memory_optimized = 0 OR memory_optimized IS NULL) then ' located on ' + groupname else '' end),
		'index_keys' = index_keys
	from #spindtab
	order by index_name


	return (0) -- sp_helpindex


/*====  SQL Server 2022 version  ====*/

create procedure sys.sp_helpindex
	@objname nvarchar(776)		-- the table to check for indexes
as
	-- PRELIM
	set nocount on

	declare @objid int,			-- the object id of the table
			@indid smallint,	-- the index id of an index
			@groupid int,  		-- the filegroup id of an index
			@indname sysname,
			@groupname sysname,
			@status int,
			@keys nvarchar(2126),	--Length (16*max_identifierLength)+(15*2)+(16*3)
			@dbname	sysname,
			@ignore_dup_key	bit,
			@is_unique		bit,
			@is_hypothetical	bit,
			@is_primary_key	bit,
			@is_unique_key 	bit,
			@is_columnstore	bit,
			@auto_created	bit,
			@no_recompute	bit,
			@memory_optimized bit, -- For hekaton tables
			@hash_index		bit -- The index is a hash index

	-- Check to see that the object names are local to the current database.
	select @dbname = parsename(@objname,3)
	if @dbname is null
		select @dbname = db_name()
	else if @dbname <> db_name()
		begin
			raiserror(15250,-1,-1)
			return (1)
		end

	-- Check to see that the table exists and initialize @objid.
	select @objid = object_id(@objname)
	if @objid is NULL
	begin
		raiserror(15009,-1,-1,@objname,@dbname)
		return (1)
	end

	-- OPEN CURSOR OVER INDEXES (skip stats: bug shiloh_51196)
	declare ms_crs_ind cursor local static for
		select i.index_id, i.data_space_id, i.name,
			i.ignore_dup_key, i.is_unique, i.is_hypothetical, i.is_primary_key, i.is_unique_constraint,
			case when (type = 5 or type = 6) then 1 else 0 end,
			case when (type = 5 or type = 6) then 0 else s.auto_created end,
			case when (type = 5 or type = 6) then 0 else s.no_recompute end,
			case when (type = 7) then 1 else 0 end
		from sys.indexes i left join sys.stats s
			on i.object_id = s.object_id and i.index_id = s.stats_id
		where i.object_id = @objid and type in (1, 2, 5, 6, 7)
	open ms_crs_ind
	fetch ms_crs_ind into @indid, @groupid, @indname, @ignore_dup_key, @is_unique, @is_hypothetical,
			@is_primary_key, @is_unique_key, @is_columnstore, @auto_created, @no_recompute, @hash_index

	-- IF NO INDEX, QUIT
	if @@fetch_status < 0
	begin
		deallocate ms_crs_ind
		raiserror(15472,-1,-1,@objname) -- Object does not have any indexes.
		return (0)
	end

	-- create temp table
	CREATE TABLE #spindtab
	(
		index_name			sysname	collate catalog_default NOT NULL,
		index_id				int,
		ignore_dup_key		bit,
		is_unique				bit,
		is_hypothetical		bit,
		is_primary_key		bit,
		is_unique_key			bit,
		is_columnstore		bit,
		auto_created			bit,
		no_recompute			bit,
		memory_optimized		bit,
		hash_index			bit,
		groupname			sysname collate catalog_default NULL,
		index_keys			nvarchar(2126)	collate catalog_default NULL -- see @keys above for length descr
	)

	-- Now check out each index, figure out its type and keys and
	--	save the info in a temporary table that we'll print out at the end.
	while @@fetch_status >= 0
	begin
		-- First we'll figure out what the keys are.
		declare @i int, @thiskey nvarchar(131) -- 128+3

		select @keys = index_col(@objname, @indid, 1), @i = 2
		if (indexkey_property(@objid, @indid, 1, 'isdescending') = 1)
			select @keys = @keys  + '(-)'

		select @thiskey = index_col(@objname, @indid, @i)
		if ((@thiskey is not null) and (indexkey_property(@objid, @indid, @i, 'isdescending') = 1))
			select @thiskey = @thiskey + '(-)'

		while (@thiskey is not null )
		begin
			select @keys = @keys + ', ' + @thiskey, @i = @i + 1
			select @thiskey = index_col(@objname, @indid, @i)
			if ((@thiskey is not null) and (indexkey_property(@objid, @indid, @i, 'isdescending') = 1))
				select @thiskey = @thiskey + '(-)'
		end

		select @groupname = null
		if (serverproperty('EngineEdition') != 5)
		select @groupname = name from sys.data_spaces where data_space_id = @groupid

		select @memory_optimized = is_memory_optimized from sys.tables where object_id = @objid

		-- INSERT ROW FOR INDEX
		insert into #spindtab values (@indname, @indid, @ignore_dup_key, @is_unique, @is_hypothetical,
			@is_primary_key, @is_unique_key, @is_columnstore, @auto_created, @no_recompute, @memory_optimized, @hash_index, @groupname, @keys)

		-- Next index
		fetch ms_crs_ind into @indid, @groupid, @indname, @ignore_dup_key, @is_unique, @is_hypothetical,
			@is_primary_key, @is_unique_key, @is_columnstore, @auto_created, @no_recompute, @hash_index
	end
	deallocate ms_crs_ind

	-- DISPLAY THE RESULTS
	select
		'index_name' = index_name,
		'index_description' = convert(varchar(210), --bits 16 off, 1, 2, 16777216 on, located on group
				case when index_id = 1 then 'clustered' else 'nonclustered' end
				+ case when hash_index = 1 then ' hash' else '' end
				+ case when ignore_dup_key <>0 then ', ignore duplicate keys' else '' end
				+ case when is_unique <>0 then ', unique' else '' end
				+ case when is_hypothetical <>0 then ', hypothetical' else '' end
				+ case when is_primary_key <>0 then ', primary key' else '' end
				+ case when is_unique_key <>0 then ', unique key' else '' end
				+ case when is_columnstore <>0 then ', columnstore' else '' end
				+ case when auto_created <>0 then ', auto create' else '' end
				+ case when no_recompute <>0 then ', stats no recompute' else '' end
				+ case when memory_optimized = 1 then ' located in MEMORY ' else '' end 
				+ case when groupname IS NOT NULL AND (memory_optimized = 0 OR memory_optimized IS NULL) then ' located on ' + groupname else '' end),
		'index_keys' = index_keys
	from #spindtab
	order by index_name


	return (0) -- sp_helpindex

